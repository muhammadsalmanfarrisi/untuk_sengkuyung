from flask import Flask, request, send_file, render_template
import pandas as pd
import os
import re
import math
import zipfile
import shutil

app = Flask(__name__)

# Pastikan folder "merged_files" ada
if not os.path.exists('merged_files'):
    os.makedirs('merged_files')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/merge', methods=['POST'])
def merge_files():
    uploaded_files = request.files.getlist('files')
    merged_data = []
    skipped_files = []

    for file in uploaded_files:
        try:
            # Baca file tanpa melewati baris untuk menemukan baris "REKAM"
            initial_df = pd.read_excel(file, None, header=None)
            sheet = list(initial_df.keys())[0]  # Gunakan sheet pertama secara default
            for i, row in initial_df[sheet].iterrows():
                if 'REKAM' in row.values:
                    start_row = i
                    break
            else:
                # Jika kolom "REKAM" tidak ditemukan, lewati file ini
                skipped_files.append(file.filename)
                continue

            # Baca file mulai dari baris yang sudah ditentukan
            df = pd.read_excel(file, skiprows=start_row, usecols=["Unnamed: 1", "REKAM", "STATUS", "Unnamed: 10", "Unnamed: 11"])

            # Ganti nama kolom sesuai permintaan
            df = df.rename(columns={
                "Unnamed: 1": "No.POLISI",
                "Unnamed: 10": "POKOK PKB",
                "Unnamed: 11": "No.HP"
            })

            # Ekstraksi bagian nomor polisi dan ubah menjadi huruf kapital
            df['nopol_awal'] = df['No.POLISI'].str.extract(r'([A-Za-z]{1,2})', expand=False)
            df['nopol_tengah'] = df['No.POLISI'].str.extract(r'([0-9]+)', expand=False)
            df['nopol_akhir'] = df['No.POLISI'].str.extract(r'([A-Za-z]+)$', expand=False)

            df['No.POLISI'] = df['No.POLISI'].str.upper()
            df['nopol_awal'] = df['nopol_awal'].str.upper()
            df['nopol_akhir'] = df['nopol_akhir'].str.upper()

            # Validasi nomor telepon dan ganti nilai non-numerik dengan 0
            df['No.HP'] = df['No.HP'].apply(lambda x: 0 if pd.isna(x) or not str(x).isdigit() else x)

            # Tambahkan dataframe ke dalam list merged_data
            merged_data.append(df)
        except Exception:
            skipped_files.append(file.filename)

    if merged_data:
        merged_df = pd.concat(merged_data, ignore_index=True)

        # Atur urutan kolom sesuai permintaan
        merged_df = merged_df[["No.POLISI", "nopol_awal", "nopol_tengah", "nopol_akhir", "REKAM", "STATUS", "POKOK PKB", "No.HP"]]

        # Bagi data menjadi beberapa file jika lebih dari 50.000 baris
        max_rows = 50000
        total_rows = len(merged_df)
        num_files = math.ceil(total_rows / max_rows)
        output_files = []

        for i in range(num_files):
            start_row = i * max_rows
            end_row = start_row + max_rows
            part_df = merged_df[start_row:end_row]

            # Simpan setiap bagian ke file terpisah
            output_file = os.path.join('merged_files', f'merged_file_part_{i+1}.xlsx')
            with pd.ExcelWriter(output_file) as writer:
                part_df.to_excel(writer, index=False, sheet_name="FEEDBACK_SENGKUYUNG")
            output_files.append(output_file)

        # Kompres file menjadi ZIP jika lebih dari satu file
        if num_files > 1:
            zip_file = os.path.join('merged_files', 'merged_files.zip')
            with zipfile.ZipFile(zip_file, 'w') as zipf:
                for file in output_files:
                    zipf.write(file, os.path.basename(file))
            response = send_file(zip_file, as_attachment=True)
        else:
            response = send_file(output_files[0], as_attachment=True)

        # Hapus semua file di dalam folder merged_files setelah diunduh
        shutil.rmtree('merged_files')
        os.makedirs('merged_files')

        return response
    else:
        return "Tidak ada file yang bisa diproses."

if __name__ == '__main__':
    app.run(debug=True)
