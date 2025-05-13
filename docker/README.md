# Docker images for UHD srsRAN and OAI

Known tags:

| Tag                     | UHD Version | srsRAN | OAI |
| ----------------------- | ----------- | ------ | --- |
| lkschu/uhd:4.0          | 4.0.0.0     | ------ | --- |
| lkschu/srsran:24.10_4.0 | 4.0.0.0     | 24.10  | --- |



## TODOS:

- [ ] Set env variable for uhd images
- [ ] pass config
- [ ] mount directory for log files


## Running srsRAN image

```bash
sudo docker run --rm -ti --privileged \
  --device /dev/bus/usb/:/dev/bus/usb/ --volume /home/gnb/:/mnt --network=host \
  lkschu/srsran:24.10_4.0 bash
```

```bash
UHD_IMAGES_DIR=/usr/share/uhd/images/ gnb -c /mnt/gnb_srs.yml
```
