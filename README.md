# Tx Steps Checker

Simple program to compute some statistics on Cairo smart contracts.

Current usage is mostly for execution from outisde transactions sent with Controller.

Some contracts are pre-registered, by you can use the CLI flags to specify the contract you want to analyze.
Current output is only in `/tmp` directory.

```bash
RUST_LOG=tx_steps_checker=trace cargo run -r -- --contract ryo
```

To then run some statistics, you can use the `scripts/stats.py` script (you may need to init python and install numpy):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install numpy matplotlib
python3 scripts/stats.py /tmp/ryo.log
```

This should show a graphic and a table with the statistics:
```bash
Count     : 1000
Mean      : 152,004
Median    : 142,548
Min       : 7,672
Max       : 523,621
Q1 (25%)  : 124,098
Q3 (75%)  : 156,565
Std Dev   : 71,359

Top 10 largest transactions by step count:
 1. 0x0510df43e130d752b06ac6a9c86e06f11d756fc15576f9ac1877fdbe38114e32 (523,621 steps)
 2. 0x05fe2a17d98faa35995a7d4394943f4038beb7e29e1fcecf2685353eb9e7cd79 (510,146 steps)
 3. 0x001944b8a54aaf21b361fc981033616e35f5633a6b477acbae0b6f65cd64fd1f (498,946 steps)
 4. 0x047f9644e823898ffcbd658a6cdb7774c403c0f31f1f3d5f7f7d1f0180b5fb1d (487,468 steps)
 5. 0x02cf6358cb494fd8c69085b449c8aba774ce59f55513bf7d54cb518a236795ef (486,819 steps)
 6. 0x00df6df541fd609a88dd715bea72f9254ad9e57506b812429758c875cd83865c (486,812 steps)
 7. 0x0564ebf6a42c9deb4126e7e69fc2230d3dc67d07cb123409fd46118087997034 (486,812 steps)
 8. 0x02bd2cb4d9c3418a0e4132e7987b2a4fc77e11026a2a4e5a28405ebb58aa4e4a (481,834 steps)
 9. 0x002d59929113d4c50aee6a0a745b9720545685ebe93e5396f32d8fa6d2a916de (478,151 steps)
10. 0x0269be74e4b81025a9989e55b56a7a0a25d895331753fccf5868f056947815a5 (478,150 steps)
```
