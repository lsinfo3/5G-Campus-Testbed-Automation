import requests
import time
import argparse

def set_gain(gain_value):
    """Helper to send the HTTP request to the siggen API."""
    url = f"http://127.0.0.1:5678/setgain?gain={gain_value}"
    try:
        # Reduced timeout as this is a local loopback call
        requests.get(url, timeout=1)
        print(f"[SET] Gain: {gain_value}")
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")

def ramp_gain_loop(start_gain, end_gain, steps, active_time, quiet_time):
    """
    Ramps gain from start to end over a specific number of steps.
    active_time: How long to stay at each gain level.
    quiet_time: How long to stay at 0 gain between steps.
    """
    # Calculate increment
    if steps > 1:
        increment = (end_gain - start_gain) / (steps - 1)
    else:
        increment = 0

    print(f"--- Configuration ---")
    print(f"Range:  {start_gain} to {end_gain}")
    print(f"Steps:  {steps} (Increment: {round(increment, 2)})")
    print(f"Timing: {active_time}s ON / {quiet_time}s OFF (at 0)")
    print(f"---------------------")
    
    try:
        while True:
            for i in range(steps):
                current_gain = round(start_gain + (increment * i), 2)
                
                # 1. Active Period
                set_gain(current_gain)
                time.sleep(active_time)
                
                # 2. Quiet Period (Reset to 0)
                set_gain(0)
                time.sleep(quiet_time)
                
    except KeyboardInterrupt:
        print("\nStopping... Resetting gain to 0.")
        set_gain(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UHD Siggen Gain Ramping Wrapper")
    
    # Define Arguments
    parser.add_argument("--start_gain", type=float, default=10.0, help="Starting gain value (default: 10.0)")
    parser.add_argument("--end_gain",   type=float, default=50.0, help="Ending gain value (default: 50.0)")
    parser.add_argument("--steps", type=int,   default=5,    help="Number of steps in the ramp (default: 5)")
    parser.add_argument("--on",    type=float, default=30.0, help="Seconds to stay at target gain (default: 30.0)")
    parser.add_argument("--off",   type=float, default=30.0, help="Seconds to stay at 0 gain (default: 30.0)")

    args = parser.parse_args()
    
    ramp_gain_loop(args.start_gain, args.end_gain, args.steps, args.on, args.off)