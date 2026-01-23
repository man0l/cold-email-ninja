import time


def main() -> None:
    # Keep the container alive for MCP-managed runs.
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
