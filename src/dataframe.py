# -*- coding: utf-8 -*-
# pip install asammdf influxdb-client pandas numpy pytz

import os, json, time
import numpy as np
import pandas as pd
import pytz
from pathlib import Path
from asammdf import MDF
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Pastas/arquivos
BASE_DIR     = Path("/home/chdt/sniffer")
ASAMMDF_DIR  = BASE_DIR / "asammdf"
DEC_DIR      = BASE_DIR / "convertido-decimal"
DF_STATE     = ASAMMDF_DIR / ".df_checkpoint.json"  # arquivos -final.mf4 já enviados

# InfluxDB
INFLUX_URL    = "http://179.106.217.150:8086"
INFLUX_ORG    = "barcoh2"
INFLUX_BUCKET = "barcoh2-belem"
INFLUX_TOKEN  = "qEBLM78UduJdJ7KXpaYuHsgvmKoVzP2DG9i5vN8lN8U7cNRsvRUnx8Q6gBBPYFF7PgiaBjgw5JIGhC0sWQjkFQ=="
DEVICE_TAG    = "3B7C3AEB"

# Timezone e estratégia de timestamp
LOCAL_TZ      = pytz.timezone("America/Manaus")  # ou "America/Belem"
USE_LOCAL_FIX = True  # True: fixa hora local; False: usa timestamps UTC direto

# Performance
CHUNK        = 200_000
MAX_CHANNELS = None  # ex.: 50 p/ teste

def load_state() -> dict:
    if DF_STATE.exists():
        try:
            return json.loads(DF_STATE.read_text())
        except Exception:
            return {"sent_files": []}
    return {"sent_files": []}

def save_state(state: dict):
    DF_STATE.parent.mkdir(parents=True, exist_ok=True)
    DF_STATE.write_text(json.dumps(state, indent=2))

def _parse_range_from_name(p: Path):
    try:
        parts = p.stem.split("-")
        first_dec = int(parts[-3])
        last_dec  = int(parts[-2])
        return first_dec, last_dec
    except Exception:
        return None, None

def pick_pending_files(state: dict) -> list[Path]:
    sent = set(state.get("sent_files", []))
    candidates = []
    for p in ASAMMDF_DIR.glob("concatenado-incr-*-final.mf4"):
        if p.name in sent:
            continue
        first_dec, last_dec = _parse_range_from_name(p)
        if first_dec is not None:
            candidates.append((last_dec, first_dec, p))
        else:
            candidates.append((int(p.stat().st_mtime), -1, p))
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return [candidates[0][2]] if candidates else []

def ensure_bucket(client: InfluxDBClient) -> None:
    api = client.buckets_api()
    buckets = api.find_buckets().buckets or []
    names = {b.name for b in buckets}
    if INFLUX_BUCKET in names:
        print(f"[INF] bucket OK: {INFLUX_BUCKET}")
        return
    org_api = client.organizations_api()
    org = next((o for o in org_api.find_organizations() if o.name == INFLUX_ORG), None)
    if org is None:
        raise RuntimeError(f"Org '{INFLUX_ORG}' não encontrada no Influx")
    api.create_bucket(bucket_name=INFLUX_BUCKET, org_id=org.id)
    print(f"[INF] bucket criado: {INFLUX_BUCKET}")

def build_name_map(mdf: MDF):
    name_map = {}
    for ch_ref in mdf.iter_channels():
        nm = ch_ref.name
        gi = (ch_ref.group_index, ch_ref.channel_index)
        if nm not in name_map:
            name_map[nm] = gi
    return name_map

def get_group_start_time_utc(mdf: MDF, sig) -> pd.Timestamp:
    try:
        src = getattr(sig, "source", None)
        if isinstance(src, tuple) and len(src) >= 1:
            g_idx = src[0]
            grp = mdf._mdf.groups[g_idx - 1]
            st = getattr(grp, "start_time", None)
            if st is not None:
                t = pd.Timestamp(st)
                return t.tz_convert("UTC") if t.tzinfo else t.tz_localize("UTC")
    except Exception:
        pass
    try:
        st2 = getattr(mdf, "start_time", None)
        if st2 is not None:
            t = pd.Timestamp(st2)
            return t.tz_convert("UTC") if t.tzinfo else t.tz_localize("UTC")
    except Exception:
        pass
    return pd.Timestamp(0, tz="UTC")

def rel_seconds_to_abs_series(rel_seconds: np.ndarray, start_time_utc: pd.Timestamp) -> pd.Series:
    rel_seconds = np.asarray(rel_seconds, dtype="float64")
    base_ns = start_time_utc.to_datetime64().astype("datetime64[ns]").astype("int64")
    rel_ns  = (rel_seconds * 1e9).astype("int64")
    abs_ns  = base_ns + rel_ns
    idx = pd.RangeIndex(len(rel_seconds))
    return pd.to_datetime(abs_ns, utc=True).to_series(index=idx)

def absolute_timestamps_from_signal(mdf: MDF, sig) -> pd.Series:
    dt_attr = getattr(sig, "timestamps_as_datetime", None)
    if dt_attr is not None:
        dt = pd.to_datetime(np.asarray(dt_attr), utc=True)
        return pd.Series(dt, index=pd.RangeIndex(len(dt)))
    try:
        sig_copy = sig.copy()
        sig_copy.convert_to_datetime()
        dt = pd.to_datetime(np.asarray(sig_copy.timestamps), utc=True)
        return pd.Series(dt, index=pd.RangeIndex(len(dt)))
    except Exception:
        pass
    start_abs = get_group_start_time_utc(mdf, sig)
    return rel_seconds_to_abs_series(sig.timestamps, start_abs)

def to_numeric_safe(v):
    if isinstance(v, (int, float, np.number)):
        return float(v)
    if isinstance(v, (bool, np.bool_)):
        return 1.0 if v else 0.0
    try:
        return float(v)
    except Exception:
        return None

def ingest_one_mf4_final(mf4_path: Path) -> tuple[int, int, int]:
    print(f"[LOAD] {mf4_path}")
    mdf = MDF(str(mf4_path))
    name_map = build_name_map(mdf)
    all_channels = list(name_map.keys())
    print(f"[INFO] canais={len(all_channels)}")

    if MAX_CHANNELS is not None:
        all_channels = all_channels[:MAX_CHANNELS]
        print(f"[INFO] limitando a {len(all_channels)} canais (teste)")

    total_pts = enviados = falhas = 0

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        ensure_bucket(client)
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for idx, name in enumerate(all_channels, start=1):
            t0c = time.time()
            try:
                g, i = name_map[name]
                sig = mdf.get(group=g, index=i, name=name)

                ts_utc = absolute_timestamps_from_signal(mdf, sig)
                vals = sig.samples
                n = len(vals)
                if n == 0:
                    print(f"[SKIP] canal={name} vazio")
                    continue

                print(f"[CANAL] ({idx}/{len(all_channels)}) {name} n={n}")
                sent_ch = 0
                discarded_val = 0
                discarded_time = 0

                for k in range(0, n, CHUNK):
                    t_part = ts_utc.iloc[k:k+CHUNK]
                    v_part = vals[k:k+CHUNK]

                    records = []
                    for t, v in zip(t_part, v_part):
                        if USE_LOCAL_FIX:
                            try:
                                local_dt = pd.Timestamp(t).tz_convert(LOCAL_TZ)
                                utc_fix  = local_dt.tz_convert("UTC")
                            except Exception:
                                discarded_time += 1
                                continue
                        else:
                            utc_fix = pd.Timestamp(t)

                        nv = to_numeric_safe(v)
                        if nv is None or not pd.notnull(nv) or not np.isfinite(nv):
                            discarded_val += 1
                            continue

                        records.append(
                            Point("can")
                              .tag("device", DEVICE_TAG)
                              .tag("signal", name)
                              .field("value", nv)
                              .time(utc_fix)
                        )

                    if records:
                        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=records)
                        enviados += len(records)
                        sent_ch += len(records)
                        print(f"[SEND] canal={name} i={min(k+CHUNK,n)}/{n} pts={len(records)} cumul={sent_ch}/{n}")

                total_pts += n
                dtc = time.time() - t0c
                print(f"[OK ] canal={name} enviados={sent_ch}/{n} desc_val={discarded_val} desc_time={discarded_time} tempo={dtc:.2f}s")

            except Exception as e:
                falhas += 1
                print(f"[ERR] canal {name}: {e}")

    print(f"[DONE] {mf4_path.name} -> canais={len(all_channels)} pts_total={total_pts} enviados={enviados} falhas={falhas}")
    return total_pts, enviados, falhas

def cleanup_on_success():
    # remove todos os .mf4 do ASAMMDF_DIR, exceto checkpoints/arquivos ocultos
    removed_asam = 0
    for p in ASAMMDF_DIR.glob("*.mf4"):
        try:
            p.unlink()
            removed_asam += 1
        except Exception as e:
            print(f"[WARN] não removeu {p.name}: {e}")
    # remove todos os .MF4 do DEC_DIR
    removed_dec = 0
    for p in DEC_DIR.glob("*.MF4"):
        try:
            p.unlink()
            removed_dec += 1
        except Exception as e:
            print(f"[WARN] não removeu {p.name}: {e}")
    print(f"[CLEAN] removidos asammdf={removed_asam} convertido-decimal={removed_dec}")

def main():
    state = load_state()
    pending = pick_pending_files(state)

    if not pending:
        print("[DF] nada pendente para enviar (checkpoint atualizado).")
        return

    print(f"[DF] pendentes: {len(pending)} arquivo(s)")
    sent = state.get("sent_files", [])
    tot_pts = tot_env = tot_err = 0

    for mf4 in pending:
        try:
            pts, env, err = ingest_one_mf4_final(mf4)
            tot_pts += pts; tot_env += env; tot_err += err
            if err == 0:
                sent.append(mf4.name)
                state["sent_files"] = sent
                save_state(state)
                print(f"[CK] marcado como enviado: {mf4.name}")
            else:
                print("[DF] houve falhas; não executar limpeza automática.")
                break
        except Exception as e:
            print(f"[ABORT] falha no arquivo {mf4.name}: {e}")
            return

    print(f"[SUMMARY] arquivos={len(pending)} pts={tot_pts} enviados={tot_env} erros={tot_err}")

    # Limpeza somente se não houve erros
    if tot_err == 0:
        cleanup_on_success()

if __name__ == "__main__":
    main()

