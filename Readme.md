Product owern: Jannis

## Epic 1: Publish BatRecroder
...
- [ ] ...

## Backlog
- [ ] Update documentation page and add maintenance information (also videos).


copy the content of BatRecorder.service to /etc/systemd/system/BatRecorder.service so we create a new system service 
```
cp BatRecorder.service /etc/systemd/system/BatRecorder.service
```

copy the bash file start_bat_recorder.sh to the home directory 
```
cp start_bat_recorder.sh /home/pi/start_bat_recorder.sh
```

make it executable
```
sudo chmod +x /home/pi/start_bat_recorder.sh
```

make the debug script form the RPi Cam executable
```
sudo chmod +x /home/pi/BatRecorder/RPi_Cam_Web_Interface/debug.sh 
```

and create a data folder to the BatRecorder
```
mkdir /home/pi/BatRecorder/data/
```

enable the system service 
```
sudo systemctl daemon-reload
sudo systemctl enable BatRecorder.service
sudo systemctl start BatRecorder
```

check log form the BatRecorder with:
```
journalctl -fu BatRecorder
```
