package org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.loadbalancer.impl.config.rev141210;

import org.opendaylight.loadbalancer.LoadbalancerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadbalancerImplModule extends org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.loadbalancer.impl.config.rev141210.AbstractLoadbalancerImplModule {

    private static final Logger LOG = LoggerFactory.getLogger(LoadbalancerImplModule.class);

    public LoadbalancerImplModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver) {
        super(identifier, dependencyResolver);
    }

    public LoadbalancerImplModule(org.opendaylight.controller.config.api.ModuleIdentifier identifier, org.opendaylight.controller.config.api.DependencyResolver dependencyResolver, org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.loadbalancer.impl.config.rev141210.LoadbalancerImplModule oldModule, java.lang.AutoCloseable oldInstance) {
        super(identifier, dependencyResolver, oldModule, oldInstance);
    }

    @Override
    public void customValidation() {
        LOG.info("Performing custom validation");
        // add custom validation form module attributes here.
    }

    @Override
    public java.lang.AutoCloseable createInstance() {
        LOG.info("Creating a new Loadbalancer instance");
        LoadbalancerImpl provider = new LoadbalancerImpl();
        getBindingAwareBrokerDependency().registerProvider(provider, null);
        return provider;
    }

}
