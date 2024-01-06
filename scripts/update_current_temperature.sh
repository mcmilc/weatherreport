#!/bin/bash
current_dir=$(pwd)
cd $HOME/projects/
$HOME/.pyenv/shims/python3 -m weatherreport.pipelines.current_temperature_ops > logs.txt
cd $current_dir
