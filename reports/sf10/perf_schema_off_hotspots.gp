set terminal svg size 1200,850 dynamic noenhanced font 'DejaVu Sans,12' background rgb 'white'
set output 'perf_schema_off_hotspots.svg'
set datafile separator "\t"
set multiplot layout 1,2 rowsfirst title 'SF10 price sort: hot leaf functions in two perf runs' font 'DejaVu Sans,17'
set style data histograms
set style histogram clustered gap 1
set style fill solid 0.9 border -1
set boxwidth 0.7
set grid ytics lc rgb '#dddddd'
set border lc rgb '#555555'
set yrange [0:20]
set ylabel 'Median share of sampled user-space cycles (%)'
set xtics rotate by -25
set key off
set title 'ClickHouse full remote sort'
plot 'perf_schema_off_server.tsv' using 2:xtic(1) lc rgb '#b45320'
set title 'DuckDB wide hybrid alpha 0.5'
plot 'perf_schema_off_client.tsv' using 2:xtic(1) lc rgb '#2369a8'
unset multiplot
