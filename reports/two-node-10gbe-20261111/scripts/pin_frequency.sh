#!/usr/bin/env bash
# Disable CPU frequency scaling: performance governor, turbo off, and the minimum frequency raised to the maximum,
# then verify the clocks under load on every CPU. Prints the previous settings so they can be restored.
set -euo pipefail
cpus=$(nproc)
echo "before: driver=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_driver)" \
     "governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)" \
     "no_turbo=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)" \
     "min=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_min_freq)" \
     "max=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq)"
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo > /dev/null
for c in /sys/devices/system/cpu/cpu[0-9]*/cpufreq; do
    echo performance | sudo tee $c/scaling_governor > /dev/null
    sudo sh -c "cat $c/scaling_max_freq > $c/scaling_min_freq"
done
echo "after:  governor=$(sort -u /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor | paste -sd,)" \
     "no_turbo=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)" \
     "min=$(sort -u /sys/devices/system/cpu/cpu*/cpufreq/scaling_min_freq | paste -sd,)" \
     "max=$(sort -u /sys/devices/system/cpu/cpu*/cpufreq/scaling_max_freq | paste -sd,)"
# Under load: one busy loop per CPU for 3 s, measured by the cycle counter.
sudo perf stat -a -e cycles -x, -o /tmp/freq.csv -- bash -c "for i in \$(seq $cpus); do timeout 3 sh -c 'while :; do :; done' & done; wait" 2> /dev/null
python3 - "$cpus" <<'EOF'
import sys
cpus = int(sys.argv[1])
cycles = next(float(l.split(",")[0]) for l in open("/tmp/freq.csv") if ",cycles" in l)
print(f"under load: {cycles / cpus / 3 / 1e9:.2f} GHz average per CPU ({cpus} CPUs busy for 3 s)")
EOF
grep MHz /proc/cpuinfo | awk '{print $4}' | sort -n | uniq -c | awk '{printf "  idle now: %s CPUs at %s MHz\n", $1, $2}'
