import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import io

st.title("Unione ed elaborazione dei datasets")

st.markdown("""
Carica i 4 dataset nei rispettivi campi
""")

# Funzione per creare file template (puoi personalizzare le intestazioni)
def crea_template(headers, formato="csv"):
    if formato == "xlsx":
        buffer = io.BytesIO()
        pd.DataFrame(columns=headers).to_excel(buffer, index=False)
        buffer.seek(0)
        return buffer.getvalue()
    else:
        output = io.StringIO()
        pd.DataFrame(columns=headers).to_csv(output, index=False)
        return output.getvalue()

# Template personalizzati
templates = {
    "Gateway attivi (Excel)": ([
        'ID LORA','SITE ID', 'Codice Torre', 'COMUNE', 'Tipologia', 'Altezza in metri', 'Altitudine',
        'Blocco Installazione', 'Antenna LORA1', 'Azimuth Antenna LORA1',
        'Coordinate GPS (Latitude)', 'Coordinate GPS (Longitude)'
    ], "xlsx"),
    "Device Gateway Performance (CSV)": ([
        'deveui', 'gatewayId', 'numero_tx', 'snr_medio', 'snr_min','snr_max'
    ], "csv"),
    "Meter A2A (CSV)": ([
        'IDTELELETTURA', 'ID', 'STATO POSA', 'LATIDUTIDE', 'LONGITUDINE', 'COMUNE', 'BLOCCO'
    ], "csv"),
    "Report Trasmissione (CSV)": ([
        'deveui', 'trasmissione'
    ], "csv")
}

# Uploader con pulsanti download template
uploaded_files = {}
for nome, (headers, formato) in templates.items():
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    with col1:
        uploaded_files[nome] = st.file_uploader(f"\U0001F4C2 {nome}", type=[formato])
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        st.download_button(
            label="\u2B07\uFE0F Scarica template",
            data=crea_template(headers, formato),
            file_name=f"{nome}.{formato}",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if formato == "xlsx" else "text/csv"
        )

# Input date
st.subheader("Inserisci la data di creazione per ciascun dataset")
data1 = st.date_input("Data - Gateway attivi")
data2 = st.date_input("Data - Device Gateway Performance")
data3 = st.date_input("Data - Meter A2A")
data4 = st.date_input("Data - Report Trasmissione")

data_finale = min(data1, data2, data3, data4)
st.session_state['data_finale'] = data_finale.strftime("%Y-%m-%d")  


# Bottone per elaborare e salvare in sessione
if st.button("Esegui l'unione dei dataset"):
    try:
        df1 = pd.read_excel(uploaded_files["Gateway attivi (Excel)"],  na_values={'COMUNE': [" ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
                        "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null"]}, keep_default_na=False)
        df2 = pd.read_csv(uploaded_files["Device Gateway Performance (CSV)"])
        df3 = pd.read_csv(uploaded_files["Meter A2A (CSV)"], sep=';', encoding='latin-1', na_values={'COMUNE': [" ", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
                          "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null"]}, keep_default_na=False)
        df4 = pd.read_csv(uploaded_files["Report Trasmissione (CSV)"], sep=';')

        df1.columns = df1.columns.str.replace('\xa0', ' ').str.strip()
        df2['gatewayId'] = df2['gatewayId'].str.upper()
        df2['deveui'] = df2['deveui'].str.upper()
        df4['deveui'] = df4['deveui'].str.upper()
        df1.rename(columns={"SITE\xa0ID": "SITE ID"}, inplace=True)

        df1_nec = df1.loc[:, templates["Gateway attivi (Excel)"][0]]
        df2_nec = df2.loc[:, templates["Device Gateway Performance (CSV)"][0]]
        df3_nec = df3.loc[:, templates["Meter A2A (CSV)"][0]]
        df4_nec = df4.loc[:, templates["Report Trasmissione (CSV)"][0]]

        join_rules = {
            ('df1_nec', 'df2_nec'): ('ID LORA', 'gatewayId'),
            ('df2_nec', 'df1_nec'): ('gatewayId', 'ID LORA'),
            ('df12', 'df3_nec'): ('deveui', 'IDTELELETTURA'),
            ('df3_nec', 'df12'): ('IDTELELETTURA', 'deveui'),
            ('df123', 'df4_nec'): ('IDTELELETTURA', 'deveui'),
            ('df4_nec', 'df123'): ('deveui', 'IDTELELETTURA')
        }

        data12 = min(data1, data2)
        left_df, right_df = (df1_nec, df2_nec) if data1 <= data2 else (df2_nec, df1_nec)
        left_name, right_name = ('df1_nec', 'df2_nec') if data1 <= data2 else ('df2_nec', 'df1_nec')
        key_left, key_right = join_rules[(left_name, right_name)]
        df12 = left_df.merge(right_df, how='left', left_on=key_left, right_on=key_right)

        data123 = min(data12, data3)
        left_df, right_df = (df12, df3_nec) if data12 <= data3 else (df3_nec, df12)
        left_name, right_name = ('df12', 'df3_nec') if data12 <= data3 else ('df3_nec', 'df12')
        key_left, key_right = join_rules[(left_name, right_name)]
        df123 = left_df.merge(right_df, how='left', left_on=key_left, right_on=key_right)

        left_df, right_df = (df123, df4_nec) if data123 <= data4 else (df4_nec, df123)
        left_name, right_name = ('df123', 'df4_nec') if data123 <= data4 else ('df4_nec', 'df123')
        key_left, key_right = join_rules[(left_name, right_name)]
        df1234 = left_df.merge(right_df, how='left', left_on=key_left, right_on=key_right)

        final_df = df1234

        col_finali = [
            'IDTELELETTURA', 'ID', 'STATO POSA', 'LATIDUTIDE', 'LONGITUDINE',
            'COMUNE_x', 'BLOCCO', 'gatewayId', 'numero_tx', 'snr_medio', 'snr_min', 'snr_max',
            'SITE ID', 'Codice Torre', 'COMUNE_y', 'Tipologia', 'Altezza in metri', 'Altitudine',
            'Antenna LORA1', 'Azimuth Antenna LORA1', 'Coordinate GPS (Latitude)', 'Coordinate GPS (Longitude)',
            'trasmissione'
        ]

        df_finale_nec = final_df.loc[:, col_finali]

        nuovi_nomi = [
            'ID TELELETTURA', 'ID', 'STATO POSA', 'LATITUDINE METER', 'LONGITUDINE METER',
            'COMUNE METER', 'BLOCCO METER', 'ID GATEWAY', 'NUMERO TX', 'SNR MEDIO', 'SNR MIN', 'SNR MAX',
            'SITE ID', 'CODICE TORRE', 'COMUNE GATEWAY', 'TIPOLOGIA', 'ALTEZZA GATEWAY [m]', 'ALTITUDINE GATEWAY [m]',
            'Antenna LORA1', 'Azimuth Antenna LORA1', 'LATITUDINE GATEWAY', 'LONGITUDINE GATEWAY', 'TRASMISSIONE METER'
        ]

        df_finale_nec.columns = nuovi_nomi

        nuovo_ordine = [
            'ID TELELETTURA', 'ID', 'STATO POSA', 'LATITUDINE METER', 'LONGITUDINE METER',
            'COMUNE METER', 'BLOCCO METER', 'TRASMISSIONE METER', 'ID GATEWAY', 'NUMERO TX', 'SNR MEDIO', 'SNR MIN', 'SNR MAX',
            'SITE ID', 'CODICE TORRE', 'COMUNE GATEWAY', 'TIPOLOGIA', 'ALTEZZA GATEWAY [m]', 'ALTITUDINE GATEWAY [m]',
            'Antenna LORA1', 'Azimuth Antenna LORA1', 'LATITUDINE GATEWAY', 'LONGITUDINE GATEWAY'
        ]

        df_finale_nec = df_finale_nec[nuovo_ordine]

        def haversine_vector(lat1, lon1, lat2, lon2):
            R = 6371  # Raggio medio della Terra in km

            # Conversione in radianti
            lat1_rad = np.radians(lat1)
            lat2_rad = np.radians(lat2)
            dlat = np.radians(lat2 - lat1)
            dlon = np.radians(lon2 - lon1)

            a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
            c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

            return R * c * 1000  # distanza in metri

        df_finale_nec['LATITUDINE METER'] = pd.to_numeric(df_finale_nec['LATITUDINE METER'], errors='coerce')
        df_finale_nec['LONGITUDINE METER'] = pd.to_numeric(df_finale_nec['LONGITUDINE METER'], errors='coerce')
        df_finale_nec['LATITUDINE GATEWAY'] = pd.to_numeric(df_finale_nec['LATITUDINE GATEWAY'], errors='coerce')
        df_finale_nec['LONGITUDINE GATEWAY'] = pd.to_numeric(df_finale_nec['LONGITUDINE GATEWAY'], errors='coerce')

        df_finale_nec['DISTANZA Meter-Gateway [m]'] = haversine_vector(
            df_finale_nec['LATITUDINE METER'],
            df_finale_nec['LONGITUDINE METER'],
            df_finale_nec['LATITUDINE GATEWAY'],
            df_finale_nec['LONGITUDINE GATEWAY']
        )


        df_posati = df_finale_nec[df_finale_nec['STATO POSA'] == 'POSATO']
        df_posati_sort = df_posati.sort_values(by=['ID TELELETTURA', 'NUMERO TX'], ascending=[True, False])
        df_limited = df_posati_sort.groupby('ID TELELETTURA').head(3).reset_index(drop=True)

        # Ordina per priorità: tra i posati, scegli il migliore per ID TELELETTURA
        df_ordinato = df_finale_nec.sort_values(by=['ID TELELETTURA', 'NUMERO TX'], ascending=[True, False])
        df_migliori = df_ordinato.groupby('ID TELELETTURA', as_index=False).head(1)
        df_noposa = df_finale_nec[df_finale_nec['STATO POSA'] != 'POSATO']
        df_selezionati = pd.concat([df_migliori, df_noposa])
        df_best_meter = df_finale_nec[df_finale_nec.index.isin(df_selezionati.index)].copy()

        # Cambia nome del Comune di Torino

        df_limited.loc[ (df_limited['COMUNE METER'] == 'Torino') & (df_limited['BLOCCO METER'] == 2), 'COMUNE METER'] = 'Torino 2'
        df_limited.loc[ (df_limited['COMUNE METER'] == 'Torino') & (df_limited['BLOCCO METER'] == 13), 'COMUNE METER'] = 'Torino 13'
        df_limited.loc[ (df_limited['COMUNE METER'] == 'Torino') & (df_limited['BLOCCO METER'] == 20), 'COMUNE METER'] = 'Torino 20'
        df_limited.loc[ (df_limited['COMUNE METER'] == 'Torino') & (df_limited['BLOCCO METER'] == 25), 'COMUNE METER'] = 'Torino 25'
        df_limited.loc[ (df_limited['COMUNE METER'] == 'Torino') & (df_limited['BLOCCO METER'] == 27), 'COMUNE METER'] = 'Torino 27'

        df_best_meter.loc[ (df_best_meter['COMUNE METER'] == 'Torino') & (df_best_meter['BLOCCO METER'] == 2), 'COMUNE METER'] = 'Torino 2'
        df_best_meter.loc[ (df_best_meter['COMUNE METER'] == 'Torino') & (df_best_meter['BLOCCO METER'] == 13), 'COMUNE METER'] = 'Torino 13'
        df_best_meter.loc[ (df_best_meter['COMUNE METER'] == 'Torino') & (df_best_meter['BLOCCO METER'] == 20), 'COMUNE METER'] = 'Torino 20'
        df_best_meter.loc[ (df_best_meter['COMUNE METER'] == 'Torino') & (df_best_meter['BLOCCO METER'] == 25), 'COMUNE METER'] = 'Torino 25'
        df_best_meter.loc[ (df_best_meter['COMUNE METER'] == 'Torino') & (df_best_meter['BLOCCO METER'] == 27), 'COMUNE METER'] = 'Torino 27'

        # Metto per prima osservazioni alfanumeriche in blocco meter, in questo modo non ho pb con csv
        df_limited['is_alphanumeric'] = ~df_limited['BLOCCO METER'].astype(str).str.isnumeric() # tilde è una negazione
        df_limited = df_limited.sort_values(by='is_alphanumeric', ascending=False)
        df_limited = df_limited.drop(columns='is_alphanumeric')

        df_best_meter['is_alphanumeric'] = ~df_best_meter['BLOCCO METER'].astype(str).str.isnumeric()
        df_best_meter = df_best_meter.sort_values(by='is_alphanumeric', ascending=False)
        df_best_meter = df_best_meter.drop(columns='is_alphanumeric')


        st.session_state['df_limited'] = df_limited
        st.session_state['df_best_meter'] = df_best_meter

        st.success("Unione completata. Ora puoi scaricare i file sotto.")
    except Exception as e:
        st.error(f"Errore durante l'elaborazione: {e}")

if 'data_finale' in st.session_state:
    data_finale = st.session_state['data_finale']

# Pulsanti download sempre visibili se i dati sono stati salvati
if 'df_limited' in st.session_state:
    buffer1 = io.BytesIO()
    st.session_state['df_limited'].to_csv(buffer1, sep=';', index=False, decimal=',', encoding="utf-8-sig")
    buffer1.seek(0)

    st.caption("Report che contiene i 3 meter posati migliori secondo il numero di trasmissioni.")
    st.download_button(
        label="\U0001F4E5 Scarica file 3 best gateway - solo posati",
        data=buffer1.getvalue(),
        file_name=f"{data_finale}_meterposatitrasmissionimigliori.csv",
        mime="text/csv"
    )

if 'df_best_meter' in st.session_state:
    buffer2 = io.BytesIO()
    st.session_state['df_best_meter'].to_csv(buffer2, sep=';', index=False, decimal=',', encoding="utf-8-sig")
    buffer2.seek(0)

    st.caption("Report che contiene tutti i meter non posati e il meter migliore a livello di numero di trasmissioni per quelli posati.")
    st.download_button(
        label="\U0001F4E5 Scarica file best meter (posati e non posati)",
        data=buffer2.getvalue(),
        file_name=f"{data_finale}_totalemetercontrasmissioni.csv",
        mime="text/csv"
    )
