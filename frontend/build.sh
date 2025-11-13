#!/bin/bash

pm2 stop 0
git pull
pnpm build
pm2 restart 0
