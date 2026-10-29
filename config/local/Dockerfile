FROM docker:29.5.2-cli@sha256:9ba8e32bfc35a2c7ae2feb1e3241b2778ae21dee80f4dcd31d04e1cfdea86ea2
WORKDIR /app
COPY --chmod=0555 bin/ /app/bin/
COPY web/ /app/web/
COPY agent-pricing.json /app/agent-pricing.json
USER 10002:10002
ENTRYPOINT ["/app/bin/launch"]
