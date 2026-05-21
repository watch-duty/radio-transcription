# Automatic Speech Recognition of Public Safety Radio Communications for Interstate Incident Detection and Notification

- **Authors:** C. M. Gartner, V. Vajpayee, J. Desai, D. M. Bullock (Purdue University)
- **Year:** 2025 (published Sep 24, 2025)
- **Venue:** Smart Cities (MDPI), Vol. 8, Issue 5, Article 157
- **URL / DOI:** https://www.mdpi.com/2624-6511/8/5/157

## Key findings
- Real deployment: monitors live **9-1-1 dispatch radio** for rural interstate incident detection using off-the-shelf hardware + open-source Whisper (turbo / large-v3).
- During May–Jun 2025, captured **>100,000 transmissions**, transcribed live within **60 s** of broadcast, producing **76 locatable events** over 71 miles of rural I-65 (Indiana), covering four county dispatch centers from one location.
- Concludes modern ASR transcribes 9-1-1 dispatch with **"usable accuracy"** for an operational incident-detection use case (not a fine-grained WER study).
- Emphasizes practicality/implementation-readiness rather than algorithmic biasing; uses Whisper largely as-is.
- Demonstrates the downstream value of transcribing short dispatch transmissions even at imperfect WER.

## Relevance
Application-level evidence that short public-safety transmissions are transcribable to operationally useful quality with stock Whisper — frames the practical bar that descriptive-prompt experiments would need to beat or at least not regress below.
