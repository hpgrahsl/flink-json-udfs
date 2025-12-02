if [ $# -lt 1 ]; then
  echo "Usage: $0 <config-file>" >&2
  exit 1
fi

CONFIG_FILE="$1"
docker run --rm \
  -p 9876:8080 \
  --env-file $(pwd)/license.env \
  -v "$(pwd)/${CONFIG_FILE}":/home/config.json \
  shadowtraffic/shadowtraffic:1.11.11 \
  --config /home/config.json \
  --stdout --watch --sample 100 --seed 1111 --with-studio
