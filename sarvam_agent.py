import os
import asyncio
import aiohttp
import pandas as pd
from datetime import datetime
from pydub import AudioSegment
from pipecat.frames.frames import Frame, AudioRawFrame, TranscriptionFrame
from pipecat.services.stt_service import STTService
from loguru import logger


class SarvamSTTService(STTService):
    """Sarvam AI Speech-to-Text service for Pipecat."""
    
    def __init__(
        self,
        *,
        api_key: str,
        model: str = "saarika:v2.5",
        language_code: str = "en-IN",
        **kwargs
    ):
        super().__init__(**kwargs)
        self._api_key = api_key
        self._model = model
        self._language_code = language_code
        self._api_url = "https://api.sarvam.ai/speech-to-text"

    async def run_stt(self, audio: bytes) -> dict:
        """Send audio to Sarvam AI and get transcription with detected language."""
        try:
            headers = {
                "api-subscription-key": self._api_key
            }
            
            data = aiohttp.FormData()
            data.add_field("model", self._model)
            
            # Don't send language_code to enable auto-detection
            if self._language_code != "auto":
                data.add_field("language_code", self._language_code)
            
            data.add_field(
                "file",
                audio,
                filename="audio.wav",
                content_type="audio/wav"
            )

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._api_url,
                    headers=headers,
                    data=data
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        transcript = result.get("transcript", "")
                        language = result.get("language_code", "unknown")
                        logger.info(f"Detected Language: {language} | Transcription: {transcript}")
                        return {
                            "transcript": transcript,
                            "language": language,
                            "error": None
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"Sarvam API error: {response.status} - {error_text}")
                        return {
                            "transcript": "", 
                            "language": "error",
                            "error": f"API Error {response.status}: {error_text}"
                        }
        except Exception as e:
            logger.error(f"Error in Sarvam STT: {e}")
            return {
                "transcript": "", 
                "language": "error",
                "error": str(e)
            }


def split_audio(file_path: str, chunk_duration_ms: int = 25000):
    """Split audio file into chunks of specified duration (default 25 seconds) as proper WAV files."""
    from io import BytesIO
    
    audio = AudioSegment.from_file(file_path)
    chunks = []
    
    for i in range(0, len(audio), chunk_duration_ms):
        chunk = audio[i:i + chunk_duration_ms]
        
        # Export chunk as WAV to BytesIO
        buffer = BytesIO()
        chunk.export(buffer, format="wav")
        chunks.append(buffer.getvalue())
    
    return chunks


async def transcribe_file(file_path: str, api_key: str, language_code: str = "auto"):
    """Transcribe an audio file using Sarvam AI with auto language detection and chunking for long files."""
    
    try:
        stt = SarvamSTTService(
            api_key=api_key,
            model="saarika:v2.5",
            language_code=language_code
        )
        
        # Check audio duration
        audio = AudioSegment.from_file(file_path)
        duration_seconds = len(audio) / 1000
        
        if duration_seconds > 30:
            logger.info(f"Audio is {duration_seconds:.1f}s, splitting into chunks...")
            chunks = split_audio(file_path)
            
            all_transcripts = []
            detected_language = "unknown"
            errors = []
            
            for idx, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {idx + 1}/{len(chunks)}")
                result = await stt.run_stt(chunk)
                
                if result.get("error"):
                    errors.append(f"Chunk {idx + 1}: {result['error']}")
                
                if result.get("transcript"):
                    all_transcripts.append(result["transcript"])
                
                if detected_language == "unknown" and result.get("language") != "error":
                    detected_language = result["language"]
            
            return {
                "transcript": " ".join(all_transcripts),
                "language": detected_language,
                "error": "; ".join(errors) if errors else None
            }
        else:
            with open(file_path, "rb") as f:
                audio_data = f.read()
            
            result = await stt.run_stt(audio_data)
            return result
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return {
            "transcript": "",
            "language": "error",
            "error": f"File processing error: {str(e)}"
        }


async def transcribe_folder(folder_path: str, api_key: str, language_code: str = "auto", output_excel: str = "transcriptions.xlsx"):
    """Transcribe all audio files in a folder with auto language detection and save to Excel."""
    
    import glob
    
    # Supported audio formats
    audio_extensions = ["*.wav", "*.mp3", "*.m4a", "*.flac", "*.ogg"]
    audio_files = []
    
    for ext in audio_extensions:
        audio_files.extend(glob.glob(os.path.join(folder_path, ext)))
    
    if not audio_files:
        logger.warning(f"No audio files found in {folder_path}")
        return []
    
    logger.info(f"Found {len(audio_files)} audio files")
    
    results = []
    for audio_file in audio_files:
        logger.info(f"\nProcessing: {audio_file}")
        result = await transcribe_file(audio_file, api_key, language_code)
        
        filename = os.path.basename(audio_file)
        error_msg = result.get("error", None)
        
        results.append({
            "File Name": filename,
            "File Path": audio_file,
            "Detected Language": result.get("language", "unknown"),
            "Transcript": result.get("transcript", ""),
            "Error": error_msg if error_msg else "Success",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    # Save to Excel
    if results:
        df = pd.DataFrame(results)
        df.to_excel(output_excel, index=False, engine='openpyxl')
        logger.info(f"\nTranscriptions saved to {output_excel}")
    
    return results


# Example usage
if __name__ == "__main__":
    # Configuration
    API_KEY = "sk_mcdsm2hx_sb0KJauF8kKdVTbkv6TBJ77b"
    INPUT_FOLDER = "input"
    OUTPUT_EXCEL = "transcriptions.xlsx"
    LANGUAGE = "auto"  # Auto-detect language (or specify: en-IN, hi-IN, etc.)
    
    # Create folder if it doesn't exist
    os.makedirs(INPUT_FOLDER, exist_ok=True)
    
    # Run transcription on all files in folder
    results = asyncio.run(transcribe_folder(INPUT_FOLDER, API_KEY, LANGUAGE, OUTPUT_EXCEL))
    
    # Print results
    print("\n" + "="*50)
    print("TRANSCRIPTION RESULTS")
    print("="*50)
    for item in results:
        print(f"\nFile: {item['File Name']}")
        print(f"Language: {item['Detected Language']}")
        print(f"Transcript: {item['Transcript']}")
        print("-"*50)
    
    print(f"\n✓ Results saved to {OUTPUT_EXCEL}")
