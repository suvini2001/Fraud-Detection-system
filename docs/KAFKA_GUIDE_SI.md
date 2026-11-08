

## 4. Run කරන විදිහ

```bash
# 1. Project folder එකට model files දාන්න
#    app/model/fraud_xgb_model.pkl, model_features.pkl, frequency_maps.pkl

# 2.
cp .env.example .env
docker compose up -d --build

# 3. Logs
docker compose logs -f scorer alerts producer

# 4. Kafka UI: http://localhost:8080   (topics, partitions, messages, consumer groups, lag)
#    API docs: http://localhost:8000/docs
```

Tests (Docker නැතුව):
```bash
pip install -r requirements-dev.txt
pytest -q
```

## 5. Demo script (interview / video එකකට)

```bash
./scripts/demo.sh topics        # Partitions 6, Leader, Replicas: 1,2,3, Isr: 1,2,3
./scripts/demo.sh groups        # scorer එක්කෙනෙක්ට partitions 2 බැගින්, LAG
./scripts/demo.sh scale 6       # REBALANCE logs, දැන් partition 1 බැගින්
./scripts/demo.sh broker-down   # kafka-2 නවත්තනවා, Isr: 1,3, producer දිගටම වැඩ
./scripts/demo.sh broker-up     # kafka-2 ආපහු ISR එකට
./scripts/demo.sh dlq           # bad message එකක් DLQ එකට යනවා
./scripts/demo.sh api           # එකම account → එකම partition (response එකේ partition number)
```

---

