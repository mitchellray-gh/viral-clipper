"""
Audio Energy Analyzer
Uses ffmpeg's astats filter to find high-energy moments in a video —
laughter spikes, gasps, crowd reactions, music peaks, raised voices.
These are strong predictors of audience engagement that text alone misses.
"""

import subprocess
import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class AudioEnergyProfile:
    """Per-second RMS energy map and detected peak moments."""
    duration: float
    energy_by_second: list[float]        # RMS energy value per second
    peak_moments: list[dict]             # [{start, end, energy, type}]
    avg_energy: float
    max_energy: float
    dynamic_range: float                 # max/avg — high value = dramatic swings

    def score_window(self, start: float, end: float) -> float:
        """
        Score a time window 0.0-1.0 by its audio energy profile.
        Rewards: high avg energy, peaks above baseline, dynamic swings.
        """
        if not self.energy_by_second or self.max_energy == 0:
            return 0.5  # neutral if no data

        start_i = max(0, int(start))
        end_i = min(len(self.energy_by_second), int(end) + 1)
        if start_i >= end_i:
            return 0.5

        window_energies = self.energy_by_second[start_i:end_i]
        if not window_energies:
            return 0.5

        window_avg = sum(window_energies) / len(window_energies)
        window_max = max(window_energies)

        # Relative to full-video baseline
        relative_avg = window_avg / max(self.avg_energy, 0.001)
        relative_max = window_max / max(self.max_energy, 0.001)

        # Count how many peaks are in this window
        peak_in_window = sum(
            1 for p in self.peak_moments
            if p["start"] >= start - 1 and p["end"] <= end + 1
        )
        peak_bonus = min(0.25, peak_in_window * 0.08)

        # Dynamic range in window (energy variance)
        if len(window_energies) > 1:
            mean = window_avg
            variance = sum((e - mean) ** 2 for e in window_energies) / len(window_energies)
            std = variance ** 0.5
            dynamic = min(0.15, std / max(self.avg_energy, 0.001) * 0.1)
        else:
            dynamic = 0.0

        score = min(1.0, relative_avg * 0.40 + relative_max * 0.35 + peak_bonus + dynamic)
        return round(score, 3)


def analyze_audio_energy(video_path: str, max_duration: float = 7200) -> AudioEnergyProfile:
    """
    Analyze the audio energy profile of a video file using ffmpeg.
    Returns per-second RMS energy and detected peak moments.
    Falls back gracefully if ffmpeg fails.
    """
    try:
        return _run_ffmpeg_analysis(video_path, max_duration)
    except Exception as e:
        logger.warning(f"Audio energy analysis failed for {video_path}: {e}")
        return AudioEnergyProfile(
            duration=0, energy_by_second=[], peak_moments=[],
            avg_energy=0.5, max_energy=1.0, dynamic_range=1.0
        )


def _run_ffmpeg_analysis(video_path: str, max_duration: float) -> AudioEnergyProfile:
    """
    Use ffmpeg's astats filter to get per-frame RMS energy,
    then downsample to per-second buckets.
    """
    # First get video duration
    probe_cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", video_path
    ]
    probe_result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=15)
    duration = max_duration
    try:
        probe_data = json.loads(probe_result.stdout)
        duration = float(probe_data.get("format", {}).get("duration", max_duration))
    except Exception:
        pass

    duration = min(duration, max_duration)

    # Use astats to get RMS energy per 1-second window
    # -reset 1 means stats reset every frame; we use -vn to skip video, -af for audio filter
    filter_str = "astats=metadata=1:reset=1,ametadata=print:key=lavfi.astats.Overall.RMS_level:file=-"
    cmd = [
        "ffmpeg", "-i", video_path,
        "-vn",                     # no video processing
        "-af", filter_str,
        "-f", "null", "-",
        "-loglevel", "quiet"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    output = result.stderr + result.stdout  # ffmpeg writes filter metadata to stderr

    energy_by_second = _parse_rms_output(output, duration)

    if not energy_by_second:
        # Fallback: use simpler volumedetect approach chunked by time
        energy_by_second = _chunked_rms_analysis(video_path, duration)

    avg_energy = sum(energy_by_second) / max(1, len(energy_by_second))
    max_energy = max(energy_by_second) if energy_by_second else 1.0
    dynamic_range = max_energy / max(avg_energy, 0.001)

    peak_moments = _detect_peaks(energy_by_second, avg_energy, max_energy)

    logger.info(f"Audio analysis: {len(energy_by_second)}s, avg={avg_energy:.3f}, "
                f"{len(peak_moments)} peak moments")

    return AudioEnergyProfile(
        duration=duration,
        energy_by_second=energy_by_second,
        peak_moments=peak_moments,
        avg_energy=avg_energy,
        max_energy=max_energy,
        dynamic_range=dynamic_range
    )


def _parse_rms_output(output: str, duration: float) -> list[float]:
    """Parse ffmpeg ametadata RMS output into per-second buckets."""
    # Pattern: lavfi.astats.Overall.RMS_level=VALUE
    # and pts_time:VALUE
    rms_values = []
    timestamps = []

    for line in output.splitlines():
        if "pts_time:" in line:
            try:
                t = float(line.split("pts_time:")[1].strip())
                timestamps.append(t)
            except Exception:
                pass
        if "lavfi.astats.Overall.RMS_level=" in line:
            try:
                val_str = line.split("=")[1].strip()
                # RMS level is in dB (negative). Convert to linear energy 0-1
                db = float(val_str)
                if db <= -91:  # silence threshold
                    rms_values.append(0.0)
                else:
                    linear = 10 ** (db / 20)  # dB to amplitude
                    rms_values.append(min(1.0, linear))
            except Exception:
                pass

    if not rms_values:
        return []

    # Downsample to 1 per second (take max in each 1-second window)
    n_seconds = int(duration) + 1
    buckets = [0.0] * n_seconds

    for i, (t, e) in enumerate(zip(timestamps, rms_values)):
        idx = min(n_seconds - 1, int(t))
        buckets[idx] = max(buckets[idx], e)

    return buckets


def _chunked_rms_analysis(video_path: str, duration: float, chunk_size: float = 1.0) -> list[float]:
    """
    Fallback: extract audio RMS per second using volumedetect on 1-second chunks.
    Slower but more compatible.
    """
    energies = []
    n_chunks = min(int(duration), 3600)  # cap at 1 hour

    for i in range(0, n_chunks, max(1, n_chunks // 200)):  # sample 200 points max
        start = i * chunk_size
        cmd = [
            "ffmpeg", "-ss", str(start), "-t", str(chunk_size),
            "-i", video_path,
            "-af", "volumedetect",
            "-vn", "-f", "null", "-",
            "-loglevel", "quiet"
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            output = res.stderr
            match = re.search(r"mean_volume:\s*([-\d.]+)\s*dB", output)
            if match:
                db = float(match.group(1))
                linear = 0.0 if db <= -91 else min(1.0, 10 ** (db / 20))
                energies.append(linear)
            else:
                energies.append(0.0)
        except Exception:
            energies.append(0.0)

    # Upsample back to full second resolution by linear interpolation
    if not energies:
        return [0.5] * int(duration)

    full = []
    step = n_chunks / max(1, len(energies))
    for s in range(int(duration)):
        idx = min(len(energies) - 1, int(s / step))
        full.append(energies[idx])

    return full


def _detect_peaks(energies: list[float], avg: float, max_e: float, threshold_factor: float = 1.5) -> list[dict]:
    """
    Find sustained high-energy moments (≥ 1.5x average, lasting ≥ 1 second).
    These correspond to laughter, cheers, gasps, raised voices.
    """
    if not energies or avg == 0:
        return []

    threshold = min(max_e * 0.75, avg * threshold_factor)
    peaks = []
    i = 0

    while i < len(energies):
        if energies[i] >= threshold:
            # Start of a peak
            start = i
            peak_val = energies[i]
            while i < len(energies) and energies[i] >= threshold * 0.8:
                peak_val = max(peak_val, energies[i])
                i += 1
            duration = i - start
            if duration >= 1:  # at least 1 second
                # Classify the peak type by energy level and duration
                relative = peak_val / max_e
                if relative > 0.85:
                    peak_type = "intense"      # very loud moment
                elif relative > 0.70:
                    peak_type = "energetic"    # excited/elevated
                else:
                    peak_type = "moderate"

                peaks.append({
                    "start": float(start),
                    "end": float(i),
                    "energy": round(peak_val, 3),
                    "type": peak_type,
                    "duration": float(duration)
                })
        else:
            i += 1

    return peaks
