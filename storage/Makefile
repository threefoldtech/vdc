.PHONY: all zerofscsi

CONTAINERIZED?=yes
CONTAINER_CMD?=$(shell docker version >/dev/null 2>&1 && echo docker)

CSI_IMAGE_NAME=$(if $(ENV_CSI_IMAGE_NAME),$(ENV_CSI_IMAGE_NAME),zerofs.csi.threefold.tech)
CSI_IMAGE_VERSION="0.1.0"
CSI_IMAGE=$(CSI_IMAGE_NAME):$(CSI_IMAGE_VERSION)

$(info czerofscsi image settings: $(CSI_IMAGE_NAME) version $(CSI_IMAGE_VERSION))
ifndef GIT_COMMIT
GIT_COMMIT=$(shell git rev-list -1 HEAD)
endif
GO_PROJECT=github.com/threefoldtech/vdc/storage

# go build flags
LDFLAGS ?=
LDFLAGS += -X $(GO_PROJECT)/GitCommit=$(GIT_COMMIT)
# CSI_IMAGE_VERSION will be considered as the driver version
LDFLAGS += -X $(GO_PROJECT)/DriverVersion=$(CSI_IMAGE_VERSION)

zerofscsi:
	GOOS=linux go build  -a -ldflags '$(LDFLAGS)' -o _output/zerofscsi

all: zerofscsi

