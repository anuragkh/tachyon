/root/tachyon-succinct/bin/th_count_test.sh 1
/root/tachyon-succinct/bin/th_count_test.sh 2
/root/tachyon-succinct/bin/th_count_test.sh 4
/root/tachyon-succinct/bin/th_count_test.sh 8
/root/tachyon-succinct/bin/th_count_test.sh 16

echo "Completed count throughput tests"

/root/tachyon-succinct/bin/th_extract_test.sh 1
/root/tachyon-succinct/bin/th_extract_test.sh 2
/root/tachyon-succinct/bin/th_extract_test.sh 4
/root/tachyon-succinct/bin/th_extract_test.sh 8
/root/tachyon-succinct/bin/th_extract_test.sh 16

echo "Completed extract throughput tests"

/root/tachyon-succinct/bin/th_locate_test.sh 1
/root/tachyon-succinct/bin/th_locate_test.sh 2
/root/tachyon-succinct/bin/th_locate_test.sh 4
/root/tachyon-succinct/bin/th_locate_test.sh 8
/root/tachyon-succinct/bin/th_locate_test.sh 16

echo "Completed locate throughput tests"