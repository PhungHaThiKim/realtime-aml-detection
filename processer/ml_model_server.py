from flask import Flask, request, jsonify
import joblib
import numpy as np
import traceback

# Load model và scaler
scaler, model = joblib.load("/app/iforest_model.pkl")

app = Flask(__name__)

@app.route("/predict", methods=["POST"])
def predict():
    try:
        data = request.json
        features = np.array([[
            data.get("tx_count_delta", 0),
            data.get("avg_amount_spike", 0),
            data.get("target_growth", 0),
            data.get("smurfing_score", 0),
            data.get("round_trip_combined", 0),
            data.get("avg_round_trip_len", 0)
        ]])

        features_scaled = scaler.transform(features)
        score = model.decision_function(features_scaled)[0]
        label = model.predict(features_scaled)[0]

        return jsonify({
            "score": float(score),
            "is_anomaly": bool(label == -1)
        })

    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
