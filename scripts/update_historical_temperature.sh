#!/bin/bash
current_dir=$(pwd)
cd ../../
python3 -m weatherreport.pipelines.historical_temperature_ops
cd $current_dir
