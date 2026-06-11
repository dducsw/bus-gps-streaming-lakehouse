import json
import numpy as np
import pandas as pd
import redis

class RedisBackedKalmanFilter:
    def __init__(self, redis_host="redis", redis_port=6379, socket_timeout=5.0, R_var=1e-6, sigma_a_sq=1e-11, max_dt=15.0):
        """
        Redis-backed Kalman Filter for GPS smoothing and velocity estimation.
        - R_var: Hardcoded measurement noise covariance fallback (default 1e-6, ~111m std dev).
                 Note: Adaptive R from Redis has been disabled due to speed sensor calibration anomalies.
        - sigma_a_sq: Process acceleration noise covariance (~0.35 m/s^2 for city bus at default 1e-11).
        - max_dt: Reset filter if time delta is larger than this threshold (seconds).
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.socket_timeout = socket_timeout
        self.R_var = R_var
        self.sigma_a_sq = sigma_a_sq
        self.max_dt = max_dt
        self.min_stationary_pings = 10   # minimum pings to update R from stationary segment
        self.speed_threshold = 2.0       # km/h — GPS device reports min 1.0 km/h when stopped

        # Kalman matrix dimensions: 4 states (x, y, vx, vy), 2 measurements (x, y)
        self.H = np.array([
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0]
        ])
        self.I = np.eye(4)
        self.R = np.array([
            [self.R_var, 0.0],
            [0.0, self.R_var]
        ])

    def get_redis_client(self):
        return redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            decode_responses=True,
            socket_timeout=self.socket_timeout
        )


    def load_state(self, redis_client, vehicle, default_x, default_y, default_timestamp):
        state_key = f"kalman_state:{vehicle}"
        state = redis_client.hgetall(state_key)
        
        if state:
            x_hat = np.array([
                [float(state["x_hat_0"])],
                [float(state["x_hat_1"])],
                [float(state["x_hat_2"])],
                [float(state["x_hat_3"])]
            ])
            P = np.array(json.loads(state["P"]))
            last_timestamp = pd.to_datetime(state["timestamp"])
            is_new = False
        else:
            # Initialize: velocity is set to 0.0
            x_hat = np.array([
                [default_x],
                [default_y],
                [0.0],
                [0.0]
            ])
            P = np.eye(4) * 0.1 # Small initial covariance for coordinates, larger for velocity will be handled
            # Set high uncertainty for velocity
            P[2, 2] = 1.0
            P[3, 3] = 1.0
            last_timestamp = pd.to_datetime(default_timestamp)
            is_new = True
            
        return x_hat, P, last_timestamp, is_new

    def save_state(self, redis_client, vehicle, x_hat, P, timestamp):
        state_key = f"kalman_state:{vehicle}"
        new_state = {
            "x_hat_0": str(x_hat[0, 0]),
            "x_hat_1": str(x_hat[1, 0]),
            "x_hat_2": str(x_hat[2, 0]),
            "x_hat_3": str(x_hat[3, 0]),
            "P": json.dumps(P.tolist()),
            "timestamp": timestamp.isoformat()
        }
        redis_client.hset(state_key, mapping=new_state)
        redis_client.expire(state_key, 7200) # 2-hour TTL

    def process_trajectory(self, pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Applies Kalman Filter over a vehicle's micro-batch dataframe.
        """
        # Ensure chronological order
        pdf = pdf.sort_values("timestamp").reset_index(drop=True)
        if len(pdf) == 0:
            return pdf

        vehicle = pdf["vehicle"].iloc[0]
        redis_client = self.get_redis_client()
        
        # Load Kalman state
        x_hat, P, last_timestamp, is_new = self.load_state(
            redis_client, 
            vehicle, 
            pdf["x"].iloc[0], 
            pdf["y"].iloc[0], 
            pdf["timestamp"].iloc[0]
        )

        # Use pre-configured measurement noise matrix R
        R = self.R

        filtered_x = []
        filtered_y = []
        filtered_speed = []

        start_idx = 0
        if is_new:
            # For the very first point of a new vehicle state, filter output = raw output
            filtered_x.append(pdf["x"].iloc[0])
            filtered_y.append(pdf["y"].iloc[0])
            # Use raw speed as fallback on initialization
            filtered_speed.append(float(pdf["speed"].iloc[0]))
            start_idx = 1
            last_timestamp = pd.to_datetime(pdf["timestamp"].iloc[0])

        times = pd.to_datetime(pdf["timestamp"])

        for i in range(start_idx, len(pdf)):
            dt = (times[i] - last_timestamp).total_seconds()
            
            # Reset filter state if dt is too large (e.g. signal loss) or negative
            if dt > self.max_dt or dt <= 0:
                x_hat[0, 0] = pdf["x"].iloc[i]
                x_hat[1, 0] = pdf["y"].iloc[i]
                x_hat[2, 0] = 0.0
                x_hat[3, 0] = 0.0
                P = np.eye(4) * 0.1
                P[2, 2] = 1.0
                P[3, 3] = 1.0
                dt = 1.0 # default time step for prediction

            # Stationary Check (Dual-Model Kalman Filter)
            raw_speed = float(pdf["speed"].iloc[i])
            # Using self.speed_threshold to capture stationary state.
            is_stationary = (raw_speed <= self.speed_threshold)

            if is_stationary:
                # Force velocity states and covariance to zero
                x_hat[2, 0] = 0.0
                x_hat[3, 0] = 0.0
                P[2, :] = 0.0
                P[:, 2] = 0.0
                P[3, :] = 0.0
                P[:, 3] = 0.0

                # F matrix is identity (constant position model)
                F = np.array([
                    [1.0, 0.0, 0.0, 0.0],
                    [0.0, 1.0, 0.0, 0.0],
                    [0.0, 0.0, 1.0, 0.0],
                    [0.0, 0.0, 0.0, 1.0]
                ])
                # Process noise is extremely small for coordinates, zero for velocity
                Q = np.zeros((4, 4))
                Q[0, 0] = 1e-12
                Q[1, 1] = 1e-12
            else:
                # Transition Matrix F
                F = np.array([
                    [1.0, 0.0, dt,  0.0],
                    [0.0, 1.0, 0.0, dt ],
                    [0.0, 0.0, 1.0, 0.0],
                    [0.0, 0.0, 0.0, 1.0]
                ])

                # Process Noise Matrix Q (based on continuous white noise acceleration model)
                dt2, dt3, dt4 = dt**2, dt**3, dt**4
                Q = np.array([
                    [(dt4/4)*self.sigma_a_sq, 0.0,                    (dt3/2)*self.sigma_a_sq, 0.0                    ],
                    [0.0,                    (dt4/4)*self.sigma_a_sq, 0.0,                    (dt3/2)*self.sigma_a_sq],
                    [(dt3/2)*self.sigma_a_sq, 0.0,                    dt2*self.sigma_a_sq,     0.0                    ],
                    [0.0,                    (dt3/2)*self.sigma_a_sq, 0.0,                    dt2*self.sigma_a_sq    ]
                ])

            # Predict Step
            x_hat = F @ x_hat
            P = F @ P @ F.T + Q

            # Update Step
            raw_x = pdf["x"].iloc[i]
            raw_y = pdf["y"].iloc[i]
            Z = np.array([[raw_x], [raw_y]])
            
            Y = Z - (self.H @ x_hat)
            S = (self.H @ P @ self.H.T) + R   # R = adaptive per-vehicle matrix
            K = P @ self.H.T @ np.linalg.inv(S)

            x_hat = x_hat + (K @ Y)
            P = (self.I - (K @ self.H)) @ P

            # Calculate smoothed speed
            if is_stationary:
                speed_kmh = 0.0
            else:
                lat_rad = np.radians(x_hat[1, 0])
                vy_mps = x_hat[3, 0] * 111320.0
                vx_mps = x_hat[2, 0] * 111320.0 * np.cos(lat_rad)
                speed_mps = np.sqrt(vx_mps**2 + vy_mps**2)
                speed_kmh = speed_mps * 3.6

                if speed_kmh > 120.0:
                    speed_kmh = float(pdf["speed"].iloc[i])
                elif speed_kmh < 0.1:
                    speed_kmh = 0.0

            filtered_x.append(x_hat[0, 0])
            filtered_y.append(x_hat[1, 0])
            filtered_speed.append(speed_kmh)
            
            last_timestamp = times[i]

        # Save latest state back to Redis
        self.save_state(redis_client, vehicle, x_hat, P, last_timestamp)
        redis_client.close()

        # Update DataFrame
        pdf["x"] = filtered_x
        pdf["y"] = filtered_y
        # Note: Raw speed is kept as it is extremely smooth and represents CAN/speedometer truth
        
        return pdf
