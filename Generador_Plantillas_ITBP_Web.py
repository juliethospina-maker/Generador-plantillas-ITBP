 # ===================================================================
# --- 1. IMPORTS (TODOS JUNTOS AL PRINCIPIO) ---
# ===================================================================
import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime
import requests
import io
import zipfile
import google.oauth2.credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

# ===================================================================
# --- 2. CONFIGURACIÓN DE CONSTANTES GLOBALES ---
# ===================================================================

# --- Configuración de OAUTH ---
# Nota: Streamlit Cloud automáticamente buscará el archivo en la carpeta .streamlit/
CLIENT_SECRETS_FILE = ".streamlit/secrets.toml" 
SCOPES = ['https://www.googleapis.com/auth/userinfo.email', 'https://www.googleapis.com/auth/userinfo.profile', 'openid']

# ¡IMPORTANTE! Reemplaza esta URL con la URL final de tu aplicación en Streamlit Cloud
REDIRECT_URI = "https://generador-plantillas-itbp.streamlit.app/" 
# Para desarrollo local, comenta la línea de arriba y descomenta la de abajo:
# REDIRECT_URI = "http://localhost:8501/"

# ¡IMPORTANTE! Reemplaza 'tuempresa.com' con el dominio de tu organización
AUTHORIZED_DOMAIN = "tuempresa.com"

# ===================================================================
# --- 3. DEFINICIÓN DE FUNCIONES ---
# ===================================================================

def create_oauth_flow():
    """Crea el objeto de flujo de OAuth a partir de los secretos."""
    # Esta función ahora puede encontrar las constantes porque están definidas antes.
    return Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )

def to_excel_buffer(df):
    """Convierte un DataFrame a un buffer de Excel en memoria."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
    return output.getvalue()

def create_zip_buffer(archivos_generados):
    """Toma una lista de (nombre_archivo, dataframe) y crea un archivo ZIP en memoria."""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for nombre_archivo, df in archivos_generados:
            excel_buffer = to_excel_buffer(df)
            zip_file.writestr(nombre_archivo, excel_buffer)
    return zip_buffer.getvalue()

# --- Funciones de Lógica de Negocio Original ---
def get_output_group_date(date):
    if date.weekday() >= 4:
        return (date + pd.Timedelta(days=6 - date.weekday())).date()
    else:
        return date.date()

def load_catalogs_from_url(url):
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        file_content = io.BytesIO(response.content)
        catalogs = {
            'itbp': pd.read_excel(file_content, sheet_name='ITBP', engine='openpyxl'),
            'txn': pd.read_excel(file_content, sheet_name='Transaction Type', engine='openpyxl'),
            'procesadora': pd.read_excel(file_content, sheet_name='Procesadora', engine='openpyxl')
        }
        if 'Pais' in catalogs['procesadora'].columns:
            catalogs['procesadora'].rename(columns={'Pais': 'País'}, inplace=True)
        return catalogs
    except Exception as e:
        st.error(f"Error al cargar catálogos: {e}")
        return None

def process_and_generate_files(df_chunk, pais_actual, grupo_fecha, catalogs):
    st.info(f"Procesando País: {pais_actual} | Fecha Grupo: {grupo_fecha.strftime('%Y-%m-%d')}...")
    # (El resto de esta función de procesamiento masivo se mantiene igual que en versiones anteriores)
    COLUMNAS_PROCESADO = ['Tipo mov.','Nº cuenta','Fecha registro','Tipo documento','Nº documento','Descripción','Importe','Importe debe','Importe haber','Cód. términos pago','Tipo de registro gen.','Nº documento externo','PostingGroup2','Prepayment','Tipo contrapartida','Cta. Contrapartida','DIM 1','DIM 2','DIM 3','DIM 4','DIM 5','DIM 6','DIM 7','DIM 8','VAT\xa0Registration\xa0Type\xa0KCP','VAT\xa0Registration\xa0No."','Cód. divisa']
    COLUMNAS_REVENUE = ['Tipo mov.','Nº cuenta','Fecha registro','Tipo documento','Nº documento','Descripción','Importe','Importe debe','Importe haber','Cód. términos pago','Tipo de registro gen.','Nº documento externo','PostingGroup2','Prepayment','Tipo contrapartida','Cta. Contrapartida','DIM 1','DIM 2','DIM 3','DIM 4','DIM 5','DIM 6','DIM 7','DIM 8','VAT\xa0Registration\xa0Type\xa0KCP','VAT\xa0Registration\xa0No."','Cód. divisa']
    df_catalogo_itbp = catalogs['itbp']
    df_catalogo_txn = catalogs['txn']
    df_catalogo_procesadora = catalogs['procesadora']
    df_cuentas_pais = df_catalogo_procesadora[df_catalogo_procesadora['País'].str.upper() == pais_actual.upper()]
    if df_cuentas_pais.empty:
        st.warning(f"ADVERTENCIA: No se encontraron cuentas para '{pais_actual}'. Omitiendo este grupo.")
        return None, None
    cuenta_procesadora = df_cuentas_pais.iloc[0]['Cuenta Contrapartida']
    Tipo_mtvo_procesadora = df_cuentas_pais.iloc[0]['Tipo mov. Contrapartida']
    VAT_Registration_Type_KCP = df_cuentas_pais.iloc[0]['VAT\xa0Registration\xa0Type\xa0KCP']
    VAT_Registration_No = df_cuentas_pais.iloc[0]['VAT\xa0Registration\xa0No."']
    df_filtrado = pd.merge(df_chunk, df_catalogo_itbp, on='merchant_id', how='inner')
    for col in ['approved_transaction_amount','kushki_commission','iva_kushki_commission']:
        if col in df_filtrado.columns:
            df_filtrado[col] = pd.to_numeric(df_filtrado[col], errors='coerce').fillna(0)
    df_totalizado = df_filtrado.groupby(['fecha_pago','createddate','merchant_id','merchant_name','currency_code','RUC_Contable_ITBP','PostingGroup2_proveedor','VAT Registration Type KCP Revenue','VAT Registration No.Revenue','TipoMovimientoCXP','DIM2','DIM3','DIM4','payment_method','transaction_type','TipoMovimientoIng','CuentaIng','CuentaIva','processor_name'], dropna=False).agg(total_approved_amount=pd.NamedAgg(column='approved_transaction_amount', aggfunc='sum'),total_kushki_commission=pd.NamedAgg(column='kushki_commission', aggfunc='sum'),total_iva_kushki_commission=pd.NamedAgg(column='iva_kushki_commission', aggfunc='sum')).reset_index()
    df_totalizado['payment_method'] = df_totalizado['payment_method'].str.upper()
    df_final = pd.merge(df_totalizado, df_catalogo_txn, on='transaction_type', how='left')
    df_final['createddate'] = pd.to_datetime(df_final['createddate'])
    df_final['Nº documento'] = 'W' + df_final['createddate'].dt.isocalendar().week.astype(str).str.zfill(2) + '-' + df_final['createddate'].dt.strftime('%y')
    if pais_actual.upper() in ['CHILE','CHILE OPERADORA']:
        dim3_valor, dim7_valor = '', df_final['DIM3']
    else:
        dim3_valor, dim7_valor = df_final['DIM3'], ''
    df_procesado = pd.DataFrame()
    df_procesado['Tipo mov.'] = df_final['TipoMovimientoCXP']
    df_procesado['Nº cuenta'] = df_final['RUC_Contable_ITBP']
    df_procesado['Fecha registro'] = df_final['createddate'].dt.strftime('%d/%m/%Y')
    df_procesado['Nº documento'] = df_final['Nº documento']
    df_procesado['Descripción'] = df_final['descripcion_txn'].fillna('') + ' ' + df_final['merchant_name'].fillna('')
    df_procesado['Importe debe'] = np.where(df_final['transaction_type'].isin(['REVERSE','CHARGEBACK','VOID','REFUND']), df_final['total_approved_amount'], 0)
    df_procesado['Importe haber'] = np.where(~df_final['transaction_type'].isin(['REVERSE','CHARGEBACK','VOID','REFUND']), df_final['total_approved_amount'], 0)
    df_procesado['Nº documento externo'] = df_final['fecha_pago'].dt.strftime('%d/%m/%Y')
    df_procesado['PostingGroup2'] = df_final['PostingGroup2_proveedor']
    df_procesado['DIM 2'] = df_final['DIM2']
    df_procesado['DIM 3'] = dim3_valor
    df_procesado['DIM 4'] = df_final['DIM4']
    df_procesado['DIM 7'] = dim7_valor
    df_procesado['Cód. divisa'] = np.where((df_final['currency_code'] == 'USD') & (pais_actual.upper() == 'PERU'), df_final['currency_code'], '')
    df_procesado['descripcion_txn'] = df_final['descripcion_txn']
    df_procesado = df_procesado[(df_procesado['Importe debe'] != 0) | (df_procesado['Importe haber'] != 0)].copy()
    resumen_por_fecha = df_procesado.groupby(['Fecha registro','Nº documento','descripcion_txn','Cód. divisa']).agg(total_debe=('Importe debe', 'sum'), total_haber=('Importe haber', 'sum')).reset_index()
    nuevas_filas_resumen = []
    doc_externo_contrapartida = '169922' if pais_actual.upper() == 'PERU' else ''
    for _, fila in resumen_por_fecha.iterrows():
        if fila['total_debe'] != 0 or fila['total_haber'] != 0:
            nuevas_filas_resumen.append({'Tipo mov.': Tipo_mtvo_procesadora,'Nº cuenta': cuenta_procesadora,'Fecha registro': fila['Fecha registro'],'Nº documento': fila['Nº documento'],'Descripción': f"{fila['descripcion_txn']} KUSHKI ACQUIRER PROCESSOR",'Importe debe': fila['total_haber'],'Importe haber': fila['total_debe'],'Nº documento externo': doc_externo_contrapartida,'VAT\xa0Registration\xa0Type\xa0KCP': VAT_Registration_Type_KCP,'VAT\xa0Registration\xa0No."': VAT_Registration_No,'Cód. divisa': fila['Cód. divisa']})
    df_procesado.drop(columns=['descripcion_txn'], inplace=True)
    df_contrapartidas_proc = pd.DataFrame(nuevas_filas_resumen)
    df_revenue = pd.DataFrame({'Tipo mov.': df_final['TipoMovimientoIng'],'Nº cuenta': df_final['CuentaIng'],'Fecha registro': df_final['createddate'].dt.strftime('%d/%m/%Y'),'Nº documento': df_final['Nº documento'],'Descripción': 'REVENUE ' + df_final['merchant_name'].fillna(''),'Importe debe': np.where(df_final['transaction_type'].isin(['REVERSE','CHARGEBACK','VOID','REFUND']), df_final['total_kushki_commission'], 0),'Importe haber': np.where(~df_final['transaction_type'].isin(['REVERSE','CHARGEBACK','VOID','REFUND']), df_final['total_kushki_commission'], 0),'Tipo de registro gen.': 'Compra' if pais_actual.upper() == 'PERU' else '','Nº documento externo' : df_final['fecha_pago'].dt.strftime('%d/%m/%Y'),'PostingGroup2': df_final['PostingGroup2_proveedor'],'Tipo contrapartida': df_final['TipoMovimientoCXP'],'Cta. Contrapartida': df_final['RUC_Contable_ITBP'],'DIM 2': df_final['DIM2'],'DIM 3': dim3_valor,'DIM 4': df_final['DIM4'],'DIM 7': dim7_valor,'VAT\xa0Registration\xa0Type\xa0KCP': df_final['VAT Registration Type KCP Revenue'],'VAT\xa0Registration\xa0No."': df_final['VAT Registration No.Revenue'],'Cód. divisa': np.where((df_final['currency_code'] == 'USD') & (pais_actual.upper() == 'PERU'), df_final['currency_code'], '')})
    df_iva = pd.DataFrame({'Tipo mov.': df_final['TipoMovimientoIng'],'Nº cuenta': df_final['CuentaIva'],'Fecha registro': df_final['createddate'].dt.strftime('%d/%m/%Y'),'Nº documento': df_final['Nº documento'],'Descripción': 'IVA REVENUE ' + df_final['merchant_name'].fillna(''),'Importe debe': np.where(df_final['transaction_type'].isin(['REVERSE','CHARGEBACK','VOID','REFUND']), df_final['total_iva_kushki_commission'], 0),'Importe haber': np.where(~df_final['transaction_type'].isin(['REVERSE','CHARGEBACK','VOID','REFUND']), df_final['total_iva_kushki_commission'], 0),'Tipo de registro gen.': 'Compra' if pais_actual.upper() == 'PERU' else '','Nº documento externo' : df_final['fecha_pago'].dt.strftime('%d/%m/%Y'),'PostingGroup2': df_final['PostingGroup2_proveedor'],'Tipo contrapartida': df_final['TipoMovimientoCXP'],'Cta. Contrapartida': df_final['RUC_Contable_ITBP'],'DIM 2': df_final['DIM2'],'DIM 3': dim3_valor,'DIM 4': df_final['DIM4'],'DIM 7': dim7_valor,'VAT\xa0Registration\xa0Type\xa0KCP': df_final['VAT Registration Type KCP Revenue'],'VAT\xa0Registration\xa0No."': df_final['VAT Registration No.Revenue'],'Cód. divisa': np.where((df_final['currency_code'] == 'USD') & (pais_actual.upper() == 'PERU'), df_final['currency_code'], '')})
    df_reporte_procesado = pd.concat([df_procesado, df_contrapartidas_proc], ignore_index=True).reindex(columns=COLUMNAS_PROCESADO).fillna('')
    df_reporte_revenue = pd.concat([df_revenue, df_iva], ignore_index=True)
    df_reporte_revenue = df_reporte_revenue[(df_reporte_revenue['Importe debe'] != 0) | (df_reporte_revenue['Importe haber'] != 0)].copy()
    df_reporte_revenue = df_reporte_revenue.reindex(columns=COLUMNAS_REVENUE).fillna('')
    for df in [df_reporte_procesado, df_reporte_revenue]:
        df['Importe debe'] = df['Importe debe'].replace(0, '')
        df['Importe haber'] = df['Importe haber'].replace(0, '')
    fecha_contable_str = grupo_fecha.strftime("%Y%m%d")
    nombre_archivo_procesado = f"Procesado_{pais_actual}_{fecha_contable_str}.xlsx"
    nombre_archivo_revenue = f"Revenue_{pais_actual}_{fecha_contable_str}.xlsx"
    return (nombre_archivo_procesado, df_reporte_procesado), (nombre_archivo_revenue, df_reporte_revenue)

# ===================================================================
# --- 4. LÓGICA PRINCIPAL DE LA APLICACIÓN (EL "PORTERO") ---
# ===================================================================

st.set_page_config(page_title="Generador ITBP", layout="wide")

# Revisa si la información del usuario ya está en la sesión
if 'user_info' not in st.session_state:
    query_params = st.query_params
    auth_code = query_params.get("code")

    if not auth_code:
        # Si no hay código, muestra el botón de login
        st.title("Bienvenido al Generador de Reportes ITBP")
        st.write("Por favor, inicia sesión con tu cuenta de Google para continuar.")
        try:
            flow = create_oauth_flow()
            authorization_url, _ = flow.authorization_url()
            # Usamos st.link_button para una mejor apariencia
            st.link_button("▶️ Iniciar sesión con Google", authorization_url, use_container_width=True)
        except FileNotFoundError:
            st.error("Archivo de secretos no encontrado. Asegúrate que '.streamlit/secrets.toml' existe.")
        except Exception as e:
            st.error(f"No se pudo crear el flujo de autenticación: {e}")

    else:
        # Si hay código, intercámbialo por un token y obtén los datos del usuario
        try:
            flow = create_oauth_flow()
            flow.fetch_token(code=auth_code)
            credentials = flow.credentials
            
            user_info_service = build('oauth2', 'v2', credentials=credentials)
            user_info = user_info_service.userinfo().get().execute()
            
            st.session_state.user_info = user_info
            st.query_params.clear()
            st.rerun()

        except Exception as e:
            st.error(f"Error durante la autenticación: {e}")
            st.stop()
else:
    # --- USUARIO AUTENTICADO ---
    user_info = st.session_state.user_info
    
    # Restricción de acceso por dominio
    if user_info.get('hd') != AUTHORIZED_DOMAIN:
        st.error(f"Acceso denegado. Por favor, inicia sesión con una cuenta de '{AUTHORIZED_DOMAIN}'.")
        if st.button("Cerrar sesión"):
            del st.session_state.user_info
            st.rerun()
    else:
        # --- LÓGICA PRINCIPAL DE LA APLICACIÓN (CONTENIDO PROTEGIDO) ---
        
        # Barra lateral con información del usuario y botón de logout
        with st.sidebar:
            st.write(f"Conectado como:")
            st.success(f"**{user_info['name']}**")
            st.write(f"({user_info['email']})")
            if st.button("Cerrar sesión"):
                del st.session_state.user_info
                st.rerun()
        
        # Contenido principal de la aplicación
        st.title("📄 Generador de Reportes ITBP")
        st.write("Esta herramienta procesa los archivos 'Detalle_liquidación' para generar los reportes de 'Procesado' y 'Revenue'.")

        if 'archivos_generados_zip' not in st.session_state:
            st.session_state.archivos_generados_zip = None

        uploaded_files = st.file_uploader(
            "Selecciona uno o más archivos 'Detalle_liquidación'",
            type="xlsx",
            accept_multiple_files=True
        )

        if uploaded_files:
            st.success(f"Cargaste {len(uploaded_files)} archivo(s). ¡Listo para procesar!")

        if st.button("🚀 Generar Reportes", disabled=not uploaded_files):
            with st.spinner("Procesando y empaquetando... Esto puede tardar unos momentos."):
                google_sheet_url = "https://docs.google.com/spreadsheets/d/1WqXYeykuKGfi1Ho5MAFGB52tRIMndIJ_/export?format=xlsx"
                st.info("Descargando catálogos...")
                catalogs = load_catalogs_from_url(google_sheet_url)

                if catalogs:
                    st.success("Catálogos cargados correctamente.")
                    lista_df_detalle = [pd.read_excel(file) for file in uploaded_files]
                    df_detalle_consolidado = pd.concat(lista_df_detalle, ignore_index=True)
                    st.info("Archivos de detalle consolidados.")
                    
                    # Lógica de negocio (ajuste para Chile)
                    condicion_descarte_mid = (df_detalle_consolidado['merchant_id'] == '20000000107065050000') & (df_detalle_consolidado['processor_name'].str.strip().str.upper() != 'KUSHKI ACQUIRER PROCESSOR')
                    df_detalle_consolidado = df_detalle_consolidado[~condicion_descarte_mid].copy()
                    condicion_kushki = (df_detalle_consolidado['country'].str.strip().str.upper() == 'CHILE') & (df_detalle_consolidado['processor_name'].str.strip().str.upper() == 'KUSHKI ACQUIRER PROCESSOR')
                    df_detalle_consolidado.loc[condicion_kushki, 'country'] = 'Chile Operadora'
                    
                    # Procesamiento principal
                    df_detalle_consolidado['createddate'] = pd.to_datetime(df_detalle_consolidado['createddate'])
                    df_detalle_consolidado['fecha_pago'] = pd.to_datetime(df_detalle_consolidado['fecha_pago'], errors='coerce')
                    df_detalle_consolidado['output_group'] = df_detalle_consolidado['createddate'].apply(get_output_group_date)
                    
                    archivos_generados = []
                    for pais in df_detalle_consolidado['country'].unique():
                        df_pais = df_detalle_consolidado[df_detalle_consolidado['country'] == pais].copy()
                        for grupo in df_pais['output_group'].unique():
                            df_chunk = df_pais[df_pais['output_group'] == grupo]
                            resultado_procesado, resultado_revenue = process_and_generate_files(df_chunk, pais, grupo, catalogs)
                            if resultado_procesado and resultado_revenue:
                                archivos_generados.append(resultado_procesado)
                                archivos_generados.append(resultado_revenue)
                    
                    if archivos_generados:
                        st.session_state.archivos_generados_zip = archivos_generados
                    else:
                        st.session_state.archivos_generados_zip = None
                        st.warning("No se generaron archivos con los datos proporcionados.")

        if st.session_state.archivos_generados_zip:
            st.success("🎉 ¡Proceso completado! 🎉")
            st.balloons()
            st.header("Descargar Todos los Archivos")
            zip_data = create_zip_buffer(st.session_state.archivos_generados_zip)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            zip_filename = f"Reportes_ITBP_{timestamp}.zip"
            st.download_button(
                label=f"📦 Descargar Todo como ZIP ({len(st.session_state.archivos_generados_zip)} archivos)",
                data=zip_data,
                file_name=zip_filename,
                mime="application/zip",
                use_container_width=True
            )
