---
title : "Linux (Debian)"
date: 2021-02-01T00:00:00Z
layout: docs
menu:
  docs:
    parent: "install"
weight: 120
---

## Download & install

Debian package files are provided so you can install Litestream and its systemd
service with a few commands. First, download the latest `.deb` file to your
local machine:

```sh
wget https://github.com/benbjohnson/litestream/releases/download/v{{< version >}}/litestream-v{{< version >}}-linux-amd64.deb
```

Then install it using `dpkg`:

```sh
sudo dpkg -i litestream-v{{< version >}}-linux-amd64.deb
```

You should now be able to run `litestream version` to verify it is installed.


## Installing as a service

To run Litestream continuously as a background service, you'll need to enable
and start the service:

```sh
sudo systemctl enable litestream
sudo systemctl start litestream
```

You can verify the service is running by checking the systemd journal:

```sh
sudo journalctl -u litestream -f
```

If you make changes to Litestream [configuration file](/reference/config),
you'll need to restart the service:

```sh
sudo systemctl restart litestream
```

