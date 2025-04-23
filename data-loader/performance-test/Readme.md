# Performance test execution for data loader

## Instructions to run the script
Execute the desired test script (Please run the tests from the root folder of the repository)
  * For import test
    ./data-loader/performance-test/import_test.sh
  * For export test
    ./data-loader/performance-test/export_test.sh

## Available command-line arguments

### Import test
```
./data-loader/performance-test/import_test.sh [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-size=size] [--image-tag=tag]
```

Example:
```
./data-loader/performance-test/import_test.sh --memory=1g,2g,4g --cpu=1,2,4 --data-size=2MB --image-tag=4.0.0-SNAPSHOT
```

### Export test
```
./data-loader/performance-test/export_test.sh [--memory=mem1,mem2,...] [--cpu=cpu1,cpu2,...] [--data-count=count] [--image-tag=tag]
```

Example:
```
./data-loader/performance-test/export_test.sh --memory=1g,2g,4g --cpu=1,2,4 --data-count=10000 --image-tag=4.0.0-SNAPSHOT
```
