# Docker images for UHD srsRAN and OAI

Known tags:

| Tag                     | UHD Version | srsRAN | OAI   |
| ----------------------- | ----------- | ------ | ----- |
| lkschu/uhd:4.0          | 4.0.0.0     | ------ | ----- |
| lkschu/srsran:24.10_4.0 | 4.0.0.0     | 24.10  | ----- |
| lkschu/oai:2.2.0_4.0    | 4.0.0.0     | ------ | 2.2.0 |



## TODOS:

- [ ] Set env variable for uhd images
- [ ] pass config
- [ ] mount directory for log files


## Running srsRAN image

The usb bus must be passed through, so the B210 can be accessed.
Set network to `host` so the local ip can be used.

Start container:
```bash
sudo docker run --rm -ti --privileged \
  --device /dev/bus/usb/:/dev/bus/usb/ \
  --network=host \
  --volume <path/to/local/config>:/srsran_config.yaml \
  lkschu/srsran:24.10_4.0

```



These are no longer needed as entrypoints are set

Inside srsran container:
```bash
UHD_IMAGES_DIR=/usr/share/uhd/images/ gnb -c /mnt/gnb_srs.yml
```

Inside oai container:
```bash
UHD_IMAGES_DIR=/usr/share/uhd/images/ /usr/local/src/OAI/cmake_targets/ran_build/build/nr-softmodem -O /mnt/gnb_oai.conf --sa --continous-tx
```
