import torchaudio
from config import TARGET_SAMPLING_RATE
from logger import logger
import time

def load_and_preprocess(path):
    start = time.time()
    try:
        waveform, sr = torchaudio.load(path)
        if sr != TARGET_SAMPLING_RATE:
            waveform = torchaudio.transforms.Resample(
                orig_freq=sr, new_freq=TARGET_SAMPLING_RATE
            )(waveform)
        if waveform.shape[0] > 1:
            waveform = waveform.mean(dim=0, keepdim=True)

        duration = time.time() - start
        logger.info(f"load_and_preprocess {path} took {duration:.3f}s")
        return waveform

    except Exception as e:
        logger.exception(f"Error in load_and_preprocess for {path}: {e}")
        raise