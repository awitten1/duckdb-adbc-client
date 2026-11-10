set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'profile_remote.svg'
set datafile separator "\t"
set multiplot layout 1,2 rowsfirst title 'SF10 remote sort: processor stages and schema probe' font 'DejaVu Sans,17'
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.9 border -1
set boxwidth 0.8
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set yrange [0:*]
set xtics rotate by -25
set key top left box opaque
set title 'Single ClickHouse processors on real query'
set ylabel 'Active seconds (stages overlap; do not add)'
plot 'profile_remote.tsv' using 2:xtic(1) title 'Sorted merge' lc rgb '#b45320', \
     '' using 3 title 'Arrow output' lc rgb '#2369a8'
set title 'DuckDB end-to-end query time'
set ylabel 'Seconds (lower is better)'
plot 'profile_schema.tsv' using 2:xtic(1) title 'Original ADBC driver' lc rgb '#7953a8', \
     '' using 3 title 'Schema-only driver test' lc rgb '#39815a'
unset multiplot
