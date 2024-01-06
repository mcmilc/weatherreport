#!/bin/bash
current_dir=$(pwd)
cd ../../
python3 -m weatherreport.pipelines.current_temperature_ops > logs.txt
cd $current_dir
