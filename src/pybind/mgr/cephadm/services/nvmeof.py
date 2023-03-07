import errno
import json
import logging
import subprocess
from typing import List, cast, Optional

from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import NvmeofServiceSpec

from orchestrator import DaemonDescription, DaemonDescriptionStatus
from .cephadmservice import CephadmDaemonDeploySpec, CephService
from .. import utils

logger = logging.getLogger(__name__)


class NvmeofService(CephService):
    TYPE = 'nvmeof'

    def config(self, spec: NvmeofServiceSpec) -> None:  # type: ignore
        assert self.TYPE == spec.service_type
        assert spec.pool
        #TODO(redo): should we create this pool? what parameters should use?
        self.mgr._check_pool_exists(spec.pool, spec.service_name())

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        spec = cast(NvmeofServiceSpec, self.mgr.spec_store[daemon_spec.service_name].spec)
        igw_id = daemon_spec.daemon_id

        #TODO(redo): fixme
        keyring = self.get_keyring_with_caps(self.get_auth_entity(igw_id),
                                             ['mon', 'allow *',
                                              'mds', 'allow *',
                                              'mgr', 'allow *',
                                              'osd', 'allow *'])
        context = {
            'spec': spec,
            'name': '{}.{}'.format(utils.name_to_config_section('nvmeof'), igw_id),
            'addr': self.mgr.get_mgr_ip(),
            'port': spec.port,
            'tgt_cmd_extra_args': None,
            'log_level': 'WARN',
            'rpc_socket': '/var/tmp/spdk.sock',
        }
        gw_conf = self.mgr.template.render('services/nvmeof/ceph-nvmeof.conf.j2', context)

        daemon_spec.keyring = keyring
        daemon_spec.extra_files = {'ceph-nvmeof.conf': gw_conf}
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)
        daemon_spec.deps = []  # TODO: which gw parameters will require a reconfig?
        return daemon_spec

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:

        # TODO(redo): fixme
        def get_set_cmd_dicts(out: str) -> List[dict]:
            gateways = json.loads(out)['gateways']
            cmd_dicts = []
            return cmd_dicts

        self._check_and_set_dashboard(
            service_name='nvmeof',
            get_cmd='dashboard nvmeof-gateway-list',
            get_set_cmd_dicts=get_set_cmd_dicts
        )

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # if only 1 nvmeof, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Nvmeof', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        # if reached here, there is > 1 nfs daemon. make sure none are down
        warn_message = ('ALERT: 1 nvmeof daemon is already down. Please bring it back up before stopping this one')
        nvmeof_daemons = self.mgr.cache.get_daemons_by_type(self.TYPE)
        for i in nvmeof_daemons:
            if i.status != DaemonDescriptionStatus.running:
                return HandleCommandResult(-errno.EBUSY, '', warn_message)

        names = [f'{self.TYPE}.{d_id}' for d_id in daemon_ids]
        warn_message = f'It is presumed safe to stop {names}'
        return HandleCommandResult(0, warn_message, '')

    def post_remove(self, daemon: DaemonDescription, is_failed_deploy: bool) -> None:
        """
        Called after the daemon is removed.
        """
        logger.debug(f'Post remove daemon {self.TYPE}.{daemon.daemon_id}')

        # remove config for dashboard nvmeof gateways
        ret, out, err = self.mgr.mon_command({
            'prefix': 'dashboard nvmeof-gateway-rm',
            'name': daemon.hostname,
        })
        if not ret:
            logger.info(f'{daemon.hostname} removed from nvmeof gateways dashboard config')

        # needed to know if we have ssl stuff for nvmeof in ceph config
        nvmeof_config_dict = {}
        ret, nvmeof_config, err = self.mgr.mon_command({
            'prefix': 'config-key dump',
            'key': 'nvmeof',
        })
        if nvmeof_config:
            nvmeof_config_dict = json.loads(nvmeof_config)

        # remove nvmeof cert and key from ceph config
        for nvmeof_key, value in nvmeof_config_dict.items():
            if f'nvmeof/client.{daemon.name()}/' in nvmeof_key:
                ret, out, err = self.mgr.mon_command({
                    'prefix': 'config-key rm',
                    'key': nvmeof_key,
                })
                logger.info(f'{nvmeof_key} removed from ceph config')

    def purge(self, service_name: str) -> None:
        """Removes configuration
        """
        spec = cast(NvmeofServiceSpec, self.mgr.spec_store[service_name].spec)
        try:
            # remove service configuration from the pool
            try:
                subprocess.run(['rados',
                                '-k', str(self.mgr.get_ceph_option('keyring')),
                                '-n', f'mgr.{self.mgr.get_mgr_id()}',
                                '-p', cast(str, spec.pool),
                                'rm',
                                'gateway.conf'],
                               timeout=5)
                logger.info(f'<gateway.conf> removed from {spec.pool}')
            except subprocess.CalledProcessError as ex:
                logger.error(f'Error executing <<{ex.cmd}>>: {ex.output}')
            except subprocess.TimeoutExpired:
                logger.error(f'timeout (5s) trying to remove <gateway.conf> from {spec.pool}')

        except Exception:
            logger.exception(f'failed to purge {service_name}')
