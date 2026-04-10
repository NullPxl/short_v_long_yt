# Basketball View Growth PCMCI Focus

These files isolate the normalized basketball view-growth PCMCI analysis only.

Interpretation guide:
- `lag_minutes`: how far the source series leads the target series in real UTC time.
- `val`: the PCMCI partial-correlation estimate on the transformed (`diff`) series.
- Positive `val` means higher source growth tends to be followed by higher target growth at that lag.
- Negative `val` means higher source growth tends to be followed by lower target growth at that lag.
- Strength labels here are rough effect-size descriptors for `|val|`: very weak < 0.05, weak 0.05-0.10, modest 0.10-0.20, moderate 0.20-0.30, strong >= 0.30.

Selected transform:
- `diff`

Significant cross-links:
## 1. Video -> Shorts at 10 min
- Plot: `01_video_to_shorts_10min.png`
- PCMCI val: `0.3061` (strong, positive)
- q-value: `1.76e-102`
- Reading: a stronger video view-growth signal is associated with higher shorts view growth about 10 minutes later, after conditioning on the two-series lag structure.

## 2. Video -> Shorts at 20 min
- Plot: `02_video_to_shorts_20min.png`
- PCMCI val: `-0.2525` (moderate, negative)
- q-value: `9.52e-69`
- Reading: a stronger video view-growth signal is associated with lower shorts view growth about 20 minutes later, after conditioning on the two-series lag structure.

## 3. Shorts -> Video at 25 min
- Plot: `03_shorts_to_video_25min.png`
- PCMCI val: `-0.2417` (moderate, negative)
- q-value: `3.65e-63`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 25 minutes later, after conditioning on the two-series lag structure.

## 4. Video -> Shorts at 5 min
- Plot: `04_video_to_shorts_5min.png`
- PCMCI val: `0.1890` (modest, positive)
- q-value: `1.17e-38`
- Reading: a stronger video view-growth signal is associated with higher shorts view growth about 5 minutes later, after conditioning on the two-series lag structure.

## 5. Shorts -> Video at 15 min
- Plot: `05_shorts_to_video_15min.png`
- PCMCI val: `-0.1845` (modest, negative)
- q-value: `6.44e-37`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 15 minutes later, after conditioning on the two-series lag structure.

## 6. Video -> Shorts at 30 min
- Plot: `06_video_to_shorts_30min.png`
- PCMCI val: `-0.1658` (modest, negative)
- q-value: `6.11e-30`
- Reading: a stronger video view-growth signal is associated with lower shorts view growth about 30 minutes later, after conditioning on the two-series lag structure.

## 7. Shorts -> Video at 30 min
- Plot: `07_shorts_to_video_30min.png`
- PCMCI val: `-0.1558` (modest, negative)
- q-value: `1.38e-26`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 30 minutes later, after conditioning on the two-series lag structure.

## 8. Shorts -> Video at 10 min
- Plot: `08_shorts_to_video_10min.png`
- PCMCI val: `-0.1490` (modest, negative)
- q-value: `1.85e-24`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 10 minutes later, after conditioning on the two-series lag structure.

## 9. Video -> Shorts at 45 min
- Plot: `09_video_to_shorts_45min.png`
- PCMCI val: `-0.1448` (modest, negative)
- q-value: `3.86e-23`
- Reading: a stronger video view-growth signal is associated with lower shorts view growth about 45 minutes later, after conditioning on the two-series lag structure.

## 10. Shorts -> Video at 40 min
- Plot: `10_shorts_to_video_40min.png`
- PCMCI val: `-0.1322` (modest, negative)
- q-value: `1.93e-19`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 40 minutes later, after conditioning on the two-series lag structure.

## 11. Shorts -> Video at 55 min
- Plot: `11_shorts_to_video_55min.png`
- PCMCI val: `0.1092` (modest, positive)
- q-value: `1.46e-13`
- Reading: a stronger shorts view-growth signal is associated with higher video view growth about 55 minutes later, after conditioning on the two-series lag structure.

## 12. Shorts -> Video at 50 min
- Plot: `12_shorts_to_video_50min.png`
- PCMCI val: `0.0965` (weak, positive)
- q-value: `8.8e-11`
- Reading: a stronger shorts view-growth signal is associated with higher video view growth about 50 minutes later, after conditioning on the two-series lag structure.

## 13. Shorts -> Video at 20 min
- Plot: `13_shorts_to_video_20min.png`
- PCMCI val: `-0.0762` (weak, negative)
- q-value: `4.76e-07`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 20 minutes later, after conditioning on the two-series lag structure.

## 14. Video -> Shorts at 40 min
- Plot: `14_video_to_shorts_40min.png`
- PCMCI val: `-0.0640` (weak, negative)
- q-value: `3.02e-05`
- Reading: a stronger video view-growth signal is associated with lower shorts view growth about 40 minutes later, after conditioning on the two-series lag structure.

## 15. Shorts -> Video at 35 min
- Plot: `15_shorts_to_video_35min.png`
- PCMCI val: `0.0627` (weak, positive)
- q-value: `4.28e-05`
- Reading: a stronger shorts view-growth signal is associated with higher video view growth about 35 minutes later, after conditioning on the two-series lag structure.

## 16. Video -> Shorts at 15 min
- Plot: `16_video_to_shorts_15min.png`
- PCMCI val: `-0.0622` (weak, negative)
- q-value: `4.56e-05`
- Reading: a stronger video view-growth signal is associated with lower shorts view growth about 15 minutes later, after conditioning on the two-series lag structure.

## 17. Shorts -> Video at 60 min
- Plot: `17_shorts_to_video_60min.png`
- PCMCI val: `-0.0542` (weak, negative)
- q-value: `0.00044`
- Reading: a stronger shorts view-growth signal is associated with lower video view growth about 60 minutes later, after conditioning on the two-series lag structure.

## 18. Video -> Shorts at 50 min
- Plot: `18_video_to_shorts_50min.png`
- PCMCI val: `0.0532` (weak, positive)
- q-value: `0.000533`
- Reading: a stronger video view-growth signal is associated with higher shorts view growth about 50 minutes later, after conditioning on the two-series lag structure.

## 19. Video -> Shorts at 35 min
- Plot: `19_video_to_shorts_35min.png`
- PCMCI val: `0.0483` (very weak, positive)
- q-value: `0.00169`
- Reading: a stronger video view-growth signal is associated with higher shorts view growth about 35 minutes later, after conditioning on the two-series lag structure.

## 20. Video -> Shorts at 25 min
- Plot: `20_video_to_shorts_25min.png`
- PCMCI val: `-0.0402` (very weak, negative)
- q-value: `0.0105`
- Reading: a stronger video view-growth signal is associated with lower shorts view growth about 25 minutes later, after conditioning on the two-series lag structure.
