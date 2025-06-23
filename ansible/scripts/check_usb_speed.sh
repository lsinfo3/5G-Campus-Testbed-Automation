#!/bin/bash



grep_string="B210"

usb_bus="$(lsusb | grep "$grep_string" | awk '{printf("%01d", $2)}')"
usb_dev="$(lsusb | grep "$grep_string" | awk '{printf("%01d", $4)}')"

# echo "Bus $usb_bus"
# echo "Dev $usb_dev"

matched_line="$(lsusb -t | sed -e '/Bus [0-9]*'"$usb_bus"'/,/\/:/!d' | grep -P 'Port [0-9]*: Dev [0-9]*'"$usb_dev" | head -n 1)"
if [[ $matched_line == "" ]]; then
    exit 13
fi

usb_driver_speed="$(rev <<< "$matched_line" | cut -d "," -f 1 | rev |xargs)"
if [[ $usb_driver_speed == "5000M" || $usb_driver_speed == "10000M" ]]; then
    echo "USB3 speed: $usb_driver_speed" 
else 
    echo "FAILURE: USB2 speed: $usb_driver_speed"
fi


