import yaml
import copy
from collections import namedtuple

# Used to build range of gains to check. E.g. BASE=12,SPREAD_AMOUNT=2,SPREAD_SIZE=3 => 6,9,12,15,18
gain_creation = namedtuple('gain_creation', 'RX_BASE TX_BASE RX_SPREAD_AMOUNT TX_SPREAD_AMOUNT RX_SPREAD_SIZE TX_SPREAD_SIZE')
srs_gains = gain_creation(RX_BASE=42, TX_BASE=74, RX_SPREAD_AMOUNT=2, TX_SPREAD_AMOUNT=2, RX_SPREAD_SIZE=4, TX_SPREAD_SIZE=4)
oai_gains = gain_creation(RX_BASE=60, TX_BASE=80, RX_SPREAD_AMOUNT=2, TX_SPREAD_AMOUNT=2, RX_SPREAD_SIZE=4, TX_SPREAD_SIZE=4)


def expand_run_definition(run:dict)->list[dict]:
    new_runs = []
    if run["gnb_version"]["type"] == "OAI":
        gains = oai_gains
    elif run["gnb_version"]["type"] == "srsRAN":
        gains = srs_gains
    else:
        raise ValueError(f"Invalide type {run["gnb_version"]["type"]}")

    for i in range(gains.RX_SPREAD_AMOUNT+1):
        if i == 0:
            r = copy.deepcopy(run)
            r["rx_gain"] = gains.RX_BASE
            new_runs.append(r)
            continue
        r = copy.deepcopy(run)
        r["rx_gain"] = gains.RX_BASE + (i)*gains.RX_SPREAD_SIZE
        r["identifier"] = f"{r["identifier"][:29]}__RX{r["rx_gain"]}{r["identifier"][29:]}"
        new_runs.append(r)
        r = copy.deepcopy(run)
        r["rx_gain"] = gains.RX_BASE - (i)*gains.RX_SPREAD_SIZE
        r["identifier"] = f"{r["identifier"][:29]}__RX{r["rx_gain"]}{r["identifier"][29:]}"
        new_runs.append(r)
    for i in range(gains.TX_SPREAD_AMOUNT+1):
        if i == 0:
            r = copy.deepcopy(run)
            r["tx_gain"] = gains.TX_BASE
            new_runs.append(r)
            continue
        r = copy.deepcopy(run)
        r["tx_gain"] = gains.TX_BASE + (i)*gains.TX_SPREAD_SIZE
        r["identifier"] = f"{r["identifier"][:29]}__RX{r["tx_gain"]}{r["identifier"][29:]}"
        new_runs.append(r)
        r = copy.deepcopy(run)
        r["tx_gain"] = gains.TX_BASE - (i)*gains.TX_SPREAD_SIZE
        r["identifier"] = f"{r["identifier"][:29]}__RX{r["tx_gain"]}{r["identifier"][29:]}"
        new_runs.append(r)
    return new_runs







def read_old_and_create_new():
    with open("./00_gain_base.yaml", "r") as f:
        old_definition = yaml.safe_load(f)
    runs = old_definition["run_definitions"]
    print(f"Read old run_definitions ({len(runs)})")
    new_runs = [ expanded_run  for r in runs for expanded_run in expand_run_definition(r) ]
    new_definitions = copy.deepcopy(old_definition)
    new_definitions["run_definitions"] = new_runs
    print(f"Created new run_definitions ({len(new_runs)})")
    with open("./10_gain_expanded.yaml", "w") as f:
        yaml.safe_dump(new_definitions,f, sort_keys=False)






if __name__ == "__main__":
    read_old_and_create_new()
