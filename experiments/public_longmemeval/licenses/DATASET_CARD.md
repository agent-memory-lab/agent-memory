---
configs:
- config_name: default
  data_files:
  - split: longmemeval_oracle
    path: longmemeval_oracle.json
  - split: longmemeval_s_cleaned
    path: longmemeval_s_cleaned.json
  - split: longmemeval_m_cleaned
    path: longmemeval_m_cleaned.json
license: mit
language:
- en
---

This dataset replaces the original LongMemEval dataset. The main difference is that this version removes noisy history sessions that interfere with the answer correctness. More detailed session processing information can be found [here](https://docs.google.com/spreadsheets/d/16cHPu2B4XhgC-VvolIoWNs8wwm0Zkbpgu8H9x-qhxWg/edit?usp=sharing).