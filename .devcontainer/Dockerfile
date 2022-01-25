# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.208.0/containers/go/.devcontainer/base.Dockerfile

# [Choice] Go version (use -bullseye variants on local arm64/Apple Silicon): 1, 1.16, 1.17, 1-bullseye, 1.16-bullseye, 1.17-bullseye, 1-buster, 1.16-buster, 1.17-buster
ARG VARIANT="1.17-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/go:0-${VARIANT}

# [Choice] Node.js version: none, lts/*, 16, 14, 12, 10
ARG NODE_VERSION="none"
RUN if [ "${NODE_VERSION}" != "none" ]; then su vscode -c "umask 0002 && . /usr/local/share/nvm/nvm.sh && nvm install ${NODE_VERSION} 2>&1"; fi

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends apt-transport-https \
    dnsutils \
    inotify-tools

RUN sed -i -E -e 's/^plugins=.*/plugins=(git golang)/' /home/vscode/.zshrc

RUN SNIPPET="export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" \
    && mkdir /commandhistory \
    && touch /commandhistory/.bash_history \
    && chown -R vscode /commandhistory \
    && echo $SNIPPET >> "/home/vscode/.bashrc"

# [Optional] Uncomment the next lines to use go get to install anything else you need
USER vscode
RUN curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.35.2
RUN GO111MODULE=on go get -v \
    golang.org/x/tools/gopls@latest \
    golang.org/x/lint/golint@latest \
    github.com/go-delve/delve/cmd/dlv@latest \
    github.com/jstemmer/go-junit-report@latest \
    github.com/axw/gocov/gocov@latest \
    github.com/AlekSi/gocov-xml \
    github.com/wadey/gocovmerge \
    gopkg.in/yaml.v2 \
    github.com/cweill/gotests/gotests \
    github.com/ory/go-acc \
    github.com/swaggo/swag/cmd/swag \
    github.com/rakyll/gotest \
    github.com/ramya-rao-a/go-outline \
    github.com/spf13/cobra/cobra \
    2>&1

# [Optional] Uncomment this line to install global node packages.
# RUN su vscode -c "source /usr/local/share/nvm/nvm.sh && npm install -g <your-package-here>" 2>&1
