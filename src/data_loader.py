import yaml
import pandas as pd
import yfinance as yf


def load_config(path="config.yaml"):
    """Legge e ritorna il contenuto di config.yaml come dict."""
    with open(path, "r") as f:
        return yaml.safe_load(f)
def download_financials(tickers, start, end):
    """
    Scarica i bilanci (financials) di ciascun ticker fra start e end,
    li unisce in un unico DataFrame e lo restituisce.
    """
    frames = []
    for t in tickers:
        # download del DataFrame transposto
        df = yf.Ticker(t).financials.T
        df = df.rename_axis("Date").reset_index()
        df["Ticker"] = t
        frames.append(df)
    return pd.concat(frames, ignore_index=True)
def load_co2_low(path="data/raw/emissions_low_granularity.csv"):
    """
    Carica il file low-granularity e ritorna un DataFrame con:
      - Year (anno)
      - parent_entity (nome azienda)
      - total_emissions_MtCO2e (milioni di t CO2)
      - Ticker (mappato dal nome azienda)
    """
    df = pd.read_csv(path)
    # Rinomino colonne per uniformità
    df = df.rename(columns={
        "year": "Year",
        "parent_entity": "Company",
        "total_emissions_MtCO2e": "Total_CO2_Mt"
    })
    # Mappa Company → Ticker da config.yaml
    mapping = {c["name"]: c["ticker"] for c in load_config()["companies"]}
    df = df[df["Company"].isin(mapping)].copy()
    df["Ticker"] = df["Company"].map(mapping)
    return df[["Ticker", "Year", "Total_CO2_Mt"]]

if __name__ == "__main__":
    # Carica la configurazione
    config = load_config()

    # Estrai ticker e periodo
    tickers = [c["ticker"] for c in config["companies"]]
    start   = config["period"]["start"]
    end     = config["period"]["end"]

    # Mostra a video per verifica
    print("Aziende:", tickers)
    print("Periodo:", start, "→", end)
    # 3) Scarica i dati finanziari
    print("Sto scaricando i bilanci da Yahoo Finance…")
    df_fin = download_financials(tickers, start, end)

    # 4) Salva su CSV
    out_path = "data/raw/financials.csv"
    df_fin.to_csv(out_path, index=False)
    print(f"Bilanci salvati in {out_path} — righe: {len(df_fin)}")

    # 5) Carica le emissioni low-granularity
    print("Caricando emissioni low-granularity…")
    df_co2 = load_co2_low()

    # 6) Prepara df_fin con Year
    df_fin["Year"] = pd.to_datetime(df_fin["Date"]).dt.year

    # 7) Merge sui campi Ticker e Year
    print("Faccio il merge dei dati…")
    df = df_co2.merge(df_fin, on=["Ticker", "Year"], how="left")

    # 8) Calcolo CO₂ per unità di fatturato
    # Nota: Total_CO2_Mt è in milioni di ton; converti in ton se vuoi
    df["CO2_per_Revenue"] = (df["Total_CO2_Mt"] * 1e6) / df["Total Revenue"]

    # 9) Salva il dataset unito
    out = "data/processed/data_merged.csv"
    df.to_csv(out, index=False)
    print(f"Dataset unito salvato in {out} — righe: {len(df)}")

