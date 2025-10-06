FROM node:20-bullseye

WORKDIR /workspace/FlowFoundry

# Pre-install frontend dependencies to warm cache
COPY foundry_web/ui/package*.json foundry_web/ui/
RUN cd foundry_web/ui && npm install

CMD ["bash", "-lc", "cd foundry_web/ui && npm install && npm run dev -- --host 0.0.0.0 --port 5173"]
