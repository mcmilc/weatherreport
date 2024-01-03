#!/bin/bash
current_dir=$(pwd)
cd ../../
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Hawthorne'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Redondo Beach'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Hermosa Beach'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'El Segundo'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Manhattan Beach'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Torrance'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Inglewood'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Gardena'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Lawndale'
python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Lomita'
cd $current_dir
