# Sarvam AI Speech-to-Text Batch Processor

Enterprise-grade batch transcription solution leveraging Sarvam AI's speech-to-text API with advanced speaker diarization and multi-language support.

## Overview

This tool provides a scalable, production-ready solution for processing large volumes of audio files with automatic language detection and speaker separation. Designed for customer service analytics, call center operations, and multilingual transcription workflows.

## Key Features

### Core Capabilities
- **Speaker Diarization** - Automatically identify and separate up to 8 speakers with timestamp precision
- **Concurrent Batch Processing** - Process 400-500 files efficiently with parallel job execution
- **Auto Language Detection** - Native support for 10+ Indian languages including English, Hindi, Telugu, Tamil, Kannada, and Malayalam
- **Asynchronous Architecture** - Non-blocking concurrent batch submission for optimal throughput
- **Structured Data Export** - Comprehensive Excel reports with speaker-separated transcripts and error tracking
- **Multi-Format Support** - Compatible with WAV, MP3, M4A, FLAC, and OGG audio formats

### Technical Highlights
- Batch size optimization (20 files per job, API limit compliant)
- Automatic retry and error handling
- Real-time progress monitoring
- Duplicate file detection
- Presigned URL-based secure file transfer

## Prerequisites

- Python 3.8 or higher
- Sarvam AI API key
- Windows/Linux/macOS

## Installation

### 1. Create Virtual Environment

**Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Linux/macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

## Quick Start

### Batch Processing (Production Use)

1. **Prepare Audio Files**
   - Place audio files in the `input` folder
   - Supported formats: `.wav`, `.mp3`, `.m4a`, `.flac`, `.ogg`

2. **Configure API Key**
   
   Edit `sarvam_batch.py`:
   ```python
   API_KEY = "your_sarvam_api_key_here"
   ```

3. **Run Batch Processor**
   ```bash
   python sarvam_batch.py
   ```

4. **Access Results**
   - Output file: `transcriptions_batch.xlsx`
   - Contains: transcripts, speaker diarization, language detection, error logs

## Configuration

### Primary Settings (`sarvam_batch.py`)

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| `API_KEY` | Sarvam AI API key | Required | Obtain from Sarvam AI dashboard |
| `INPUT_FOLDER` | Audio files directory | `"input"` | Relative or absolute path |
| `OUTPUT_EXCEL` | Results filename | `"transcriptions_batch.xlsx"` | Excel format |
| `BATCH_SIZE` | Files per batch job | `20` | Sarvam API limit |
| `NUM_SPEAKERS` | Expected speakers | `2` | Customer + Agent typical |

## Output Schema

### Excel File Structure

| Column | Type | Description |
|--------|------|-------------|
| File Name | String | Audio filename |
| File Path | String | Full file path |
| Detected Language | String | ISO language code (e.g., `hi-IN`, `te-IN`) |
| Transcript | Text | Complete transcript without speaker labels |
| Diarized Transcript | Text | Speaker-separated transcript with timestamps |
| Error | String | `"Success"` or error message |
| Timestamp | DateTime | Processing completion time |

### Diarized Transcript Format

```
[0.5s] Customer: Hello, I need help with my electricity connection
[3.2s] Agent: Sure, can you provide your account number?
[5.8s] Customer: It's 123456789
[8.1s] Agent: Thank you, let me check that for you
```

## Performance Metrics

| Files | Batches | Concurrent Jobs | Est. Time* |
|-------|---------|-----------------|------------|
| 100   | 5       | 5               | ~2-3 min   |
| 300   | 15      | 15              | ~3-5 min   |
| 500   | 25      | 25              | ~5-8 min   |

*Estimated time depends on audio duration and API response time

## Supported Languages

### Indian Languages
- English (en-IN)
- Hindi (hi-IN)
- Telugu (te-IN)
- Tamil (ta-IN)
- Kannada (kn-IN)
- Malayalam (ml-IN)
- Bengali (bn-IN)
- Gujarati (gu-IN)
- Marathi (mr-IN)
- Punjabi (pa-IN)

## Error Handling

### Common Issues

**Issue:** `Maximum number of audio files per job is 20`
- **Solution:** Batch size automatically set to 20 (API limit)

**Issue:** `No audio files found`
- **Solution:** Verify files are in `input` folder with supported extensions

**Issue:** `API Error 403`
- **Solution:** Check API key validity and account status

## Best Practices

1. **File Organization** - Use consistent naming conventions for audio files
2. **Batch Size** - Keep at 20 files per batch (API limit)
3. **Speaker Count** - Set `NUM_SPEAKERS` accurately for better diarization
4. **API Key Security** - Use environment variables in production
5. **Monitoring** - Review error column in Excel for failed transcriptions

## License

MIT

---

**Built for scalable multilingual transcription**
