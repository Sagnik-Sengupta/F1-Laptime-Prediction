import fastf1
import pandas as pd
import os
import time

def safe_load(session, load_kwargs, attempts=3, delay=2):
    """
    Attempts to load a FastF1 session using the provided load_kwargs.
    Retries up to `attempts` times with a delay between attempts.
    """
    for attempt in range(attempts):
        try:
            session.load(**load_kwargs)
            return True
        except Exception as e:
            if attempt < attempts - 1:
                print(f"Retry {attempt+1}/{attempts} for session {session.event['EventName']} {session.name} due to error: {e}. Waiting {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"Failed to load session {session.event['EventName']} {session.name} after {attempts} attempts: {e}")
                return False

def get_event_rounds(year):
    """
    Returns a list of valid rounds for a given season by using FastF1's event schedule.
    If the schedule cannot be retrieved, returns a default range of rounds (1 to 25).
    """
    try:
        schedule = fastf1.get_event_schedule(year)
        rounds = schedule['Round'].dropna().unique().tolist()
        rounds.sort()
        print(f"Found rounds for {year}: {rounds}")
        return rounds
    except Exception as e:
        print(f"Could not retrieve event schedule for {year}: {e}. Using default round range.")
        return list(range(1, 25))

def get_best_fp_times(year, round_number, fp_session_name):
    """
    Get the best lap time per driver from the specified free practice session.
    Returns a DataFrame with 'DriverNumber' and the best lap time (in seconds)
    under the column name corresponding to the fp_session_name.
    """
    try:
        session = fastf1.get_session(year, round_number, fp_session_name)
        # Use safe_load with retries; include laps=True
        if not safe_load(session, {'laps': True, 'telemetry': False, 'weather': False}):
            raise Exception("Session failed to load after retries.")
        best_laps = session.laps.groupby('DriverNumber')['LapTime'].min().reset_index()
        best_laps[fp_session_name] = best_laps['LapTime'].apply(lambda x: x.total_seconds())
        best_laps.drop(columns='LapTime', inplace=True)
        return best_laps
    except Exception as e:
        print(f"Warning: {fp_session_name} data not available for {year} Round {round_number}: {e}")
        return pd.DataFrame(columns=['DriverNumber', fp_session_name])

def fetch_f1_data(year, round_number):
    """
    Fetch qualifying data and best free practice lap times (FP1, FP2, FP3) for a given race.
    Returns a DataFrame with columns:
    DriverNumber, Driver, TeamName, FP1, FP2, FP3, Q1, Q2, Q3, Year, Round.
    """
    try:
        q_session = fastf1.get_session(year, round_number, 'Q')
        if not safe_load(q_session, {}):
            raise Exception("Qualifying session failed to load.")
        
        q_results = q_session.results[['DriverNumber', 'FullName', 'TeamName', 'Q1', 'Q2', 'Q3']].copy()
        q_results = q_results.rename(columns={'FullName': 'Driver'})
        for col in ['Q1', 'Q2', 'Q3']:
            q_results[col] = q_results[col].apply(lambda x: x.total_seconds() if pd.notnull(x) else None)
        
        q_results['Year'] = year
        q_results['Round'] = round_number

        for fp in ['FP1', 'FP2', 'FP3']:
            fp_times = get_best_fp_times(year, round_number, fp)
            q_results = pd.merge(q_results, fp_times, on='DriverNumber', how='left')
        
        return q_results
    except Exception as e:
        print(f"Error fetching qualifying data for {year} Round {round_number}: {e}")
        return None

def save_data_to_csv(data, filename):
    """Save DataFrame to CSV."""
    if not data.empty:
        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
    else:
        print("No data to save.")

if __name__ == "__main__":
    fastf1.Cache.enable_cache('cache')
    year = 2022  # Seasons 2018 through 2024

    print(f"\nProcessing season {year}...")
    season_data = []
    rounds = get_event_rounds(year)
    for round_number in rounds:
        print(f"  Fetching data for {year} Round {round_number}...")
        df = fetch_f1_data(year, round_number)
        if df is not None:
            season_data.append(df)
        # Optional delay between rounds to reduce pressure on the server
        time.sleep(1)
    if season_data:
        combined_season_data = pd.concat(season_data, ignore_index=True)
        filename = f"f1_data_{year}.csv"
        save_data_to_csv(combined_season_data, filename)
    else:
        print(f"No data collected for season {year}.")
