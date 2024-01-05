#!/bin/bash
current_dir=$(pwd)
cd $HOME/projects/
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Hawthorne'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Redondo Beach'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Hermosa Beach'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'El Segundo'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Manhattan Beach'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Torrance'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Inglewood'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Gardena'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Lawndale'
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.historical_temperature_ops -c 'Lomita'
cd $current_dir
