set terminal svg size 1100,630 dynamic font 'DejaVu Sans,15'
set datafile commentschars '#'
set key outside top center horizontal
set grid ytics lc rgb '#d8dee8'
set border 3 lc rgb '#606b7a'
set tics nomirror
set ylabel 'EXPLAIN ANALYZE query time (s)'

set output 'path_times.svg'
set title 'SF50 orders: sort path by key (9 columns, 9 GiB DuckDB)'
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.88 border -1
set boxwidth 0.85
set xtics rotate by -20
plot 'path_times.tsv' using 2:xtic(1) title 'DuckDB sort' lc rgb '#3066be', \
     '' using 3 title 'ClickHouse sort' lc rgb '#dd7437', \
     '' using 4 title 'Hybrid α=0.5' lc rgb '#7c5ca7', \
     '' using 5 title 'Hybrid α=0.75' lc rgb '#3c9d83'

set output 'alpha_sweep.svg'
set title 'SF50 hybrid: alpha sweep (9 columns, 9 GiB DuckDB)'
unset style data
set xtics 0,0.25,1 rotate by 0
set xrange [-0.02:1.02]
set yrange [0:70]
set xlabel 'Alpha / fraction sent to ClickHouse'
set style line 1 lc rgb '#dd7437' lw 3 pt 7 ps 1.2
set style line 2 lc rgb '#3066be' lw 3 pt 7 ps 1.2
plot 'alpha_sweep.tsv' using 1:2 with linespoints ls 2 title 'o_orderdate', \
     '' using 1:3 with linespoints ls 1 title 'o_totalprice', \
     7.481 with lines lc rgb '#3066be' dt 2 title 'Date: ClickHouse sort', \
     9.013 with lines lc rgb '#dd7437' dt 2 title 'Price: DuckDB sort'

set output 'memory.svg'
set title 'SF50: larger DuckDB memory cap removes sort spill'
set xlabel 'DuckDB memory cap (GiB)'
set xrange [7:22]
set xtics (9,20)
set yrange [0:20]
set style line 3 lc rgb '#3c9d83' lw 3 pt 7 ps 1.2
set style line 4 lc rgb '#7c5ca7' lw 3 pt 7 ps 1.2
plot 'memory.tsv' using (strcol(1) eq 'orderdate' ? $2 : 1/0):3 with linespoints ls 2 title 'Date: DuckDB sort', \
     '' using (strcol(1) eq 'orderdate' ? $2 : 1/0):4 with linespoints ls 4 title 'Date: hybrid α=0.5', \
     '' using (strcol(1) eq 'totalprice' ? $2 : 1/0):3 with linespoints ls 3 title 'Price: DuckDB sort', \
     '' using (strcol(1) eq 'totalprice' ? $2 : 1/0):4 with linespoints ls 1 title 'Price: hybrid α=0.5'
