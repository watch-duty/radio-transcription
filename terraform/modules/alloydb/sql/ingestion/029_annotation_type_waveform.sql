-- Add WAVEFORM to the ANNOTATION_TYPE enum so waveform peaks can be stored as annotations.
ALTER TYPE annotation_type ADD VALUE IF NOT EXISTS 'WAVEFORM';
