//! Live audio level (RMS) for a volume meter in the UI.

/// Computes the peak/RMS-ish level of a `pcm_s16le` mono chunk, normalized
/// to `0.0..=1.0`. Used to drive a live volume meter so the UI can render
/// input activity without capturing anything itself.
pub fn rms_level(chunk: &[u8]) -> f64 {
    if chunk.len() < 2 {
        return 0.0;
    }
    let samples = chunk.chunks_exact(2);
    let mut sum_squares = 0f64;
    let mut count = 0usize;
    for pair in samples {
        let sample = i16::from_le_bytes([pair[0], pair[1]]) as f64;
        sum_squares += sample * sample;
        count += 1;
    }
    if count == 0 {
        return 0.0;
    }
    let rms = (sum_squares / count as f64).sqrt();
    // Normalize against i16 max amplitude (32768), then apply a gentle gain so
    // quiet speech still produces a visible meter.
    (rms / 32768.0 * 5.0).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silence_is_zero() {
        let chunk = vec![0u8; 640]; // 320 samples of 0
        assert_eq!(rms_level(&chunk), 0.0);
    }

    #[test]
    fn full_scale_is_one() {
        // i16::MAX amplitude on every sample -> normalized to ~1.0 after gain.
        let chunk: Vec<u8> = i16::MAX.to_le_bytes().into_iter().chain([0u8; 0]).collect();
        assert!((0.9..=1.0).contains(&rms_level(&chunk)));
    }

    #[test]
    fn odd_length_is_ignored() {
        // The odd trailing byte cannot form a full sample and is dropped.
        // A two-byte zero sample followed by one junk byte still scores 0.
        let chunk = vec![0u8, 0u8, 0xffu8];
        assert_eq!(rms_level(&chunk), 0.0);
    }

    #[test]
    fn known_value() {
        // A single sample = 3276 -> RMS 3276/32768*5 = 0.5 approx.
        let chunk: Vec<u8> = vec![0; 640]
            .chunks_exact_mut(2)
            .flat_map(|_| (3276i16).to_le_bytes())
            .collect();
        let level = rms_level(&chunk);
        assert!((0.49..=0.51).contains(&level));
    }
}
