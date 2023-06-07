# Tugas Besar 1 IF3230 Sistem Paralel dan Terdistribusi
## Anggota Kelompok
1. 13520120 - Afrizal Sebastian
2. 13520121 - Nicholas Budiono
3. 13520130 - Nelsen Putra
4. 13520167 - Aldwin Hardi Swastia

## Deskripsi Singkat
Program ini merupakan implementasi algoritma RAFT pada sistem terdistribusi. Program terdiri dari client dan server. Client dapat mengirimkan request ke server, dan server akan memproses request tersebut. Server akan mengirimkan response ke client apabila request telah diproses. Program ini dibuat menggunakan bahasa pemrograman Python. 

## Requirement Program
1. Python

## Petunjuk Penggunaan Program
1. Buka terminal
2. Jalankan server dengan perintah `python server.py <ip> <port>`
3. Buka terminal baru
4. Tambahkan node dengan perintah `python client.py <ip> <port> <ip_server> <port_server>`
5. Jalankan client dengan perintah `python client.py <ip> <port>`
6. Masukkan perintah yang ingin dijalankan pada client