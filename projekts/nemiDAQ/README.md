# nemiDAQ (offline decoder + live view)

## Run
```bash
pip install -r requirements.txt
python main.py
```

## Tabs
- **Live**: COM-port streaming, receiver initialization, live plot, optional manual CSV recording,
  and optional auto-trigger event recording (pre/post window).
- **Offline**: BIN decode, CSV overlay/compare, export.

## Notes
- RF preset bytes are best-effort guesses except for `0x08` (observed in captured CFG1 and
  assumed to match HS 2 Mbit/s). If your device uses different bytes, choose **Custom**
  and set the byte manually.
