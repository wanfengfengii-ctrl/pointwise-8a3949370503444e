from warehouse.mirror.apply import InstallResult, apply_release
from warehouse.mirror.repository import MirrorRepository
from warehouse.mirror.verification import verify_artifact


def install_release(repository: MirrorRepository, artifact: bytes) -> InstallResult:
    return apply_release(repository, verify_artifact(artifact))
