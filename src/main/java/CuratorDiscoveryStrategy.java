import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.UriSpec;

import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.config.properties.SimplePropertyDefinition;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import static com.hazelcast.config.properties.PropertyTypeConverter.STRING;
import static java.util.stream.Collectors.toList;

public class CuratorDiscoveryStrategy extends AbstractDiscoveryStrategy {

    public static final PropertyDefinition GROUP = new SimplePropertyDefinition("group", false, STRING);

    private static final String DEFAULT_GROUP_NAME = "hazelcast";

    private final ServiceDiscovery<Void> serviceDiscovery;

    private final DiscoveryNode thisNode;

    private final ILogger logger;

    private String group;

    private ServiceInstance<Void> serviceInstance;

    public CuratorDiscoveryStrategy(ServiceDiscovery serviceDiscovery, DiscoveryNode discoveryNode, ILogger logger,
                                    Map<String, Comparable> properties) {
        super(logger, properties);
        this.logger = logger;
        this.serviceDiscovery = serviceDiscovery;
        this.thisNode = discoveryNode;
    }

    @Override
    public void start() {
        group = getOrDefault(GROUP, DEFAULT_GROUP_NAME);
        try {
            serviceInstance = buildServiceInstance(thisNode.getPrivateAddress());
            serviceDiscovery.registerService(serviceInstance);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot register this node as a service", e);
        }
    }

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        Collection<ServiceInstance<Void>> members;
        try {
            members = serviceDiscovery.queryForInstances(group);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot query for members of group: " + group, e);
        }
        List<DiscoveryNode> nodes = members.stream().map(this::prepareDiscoveryNode).collect(toList());
        logger.info("Discovered instance(s): " + nodes.stream().map(node -> node.getPrivateAddress().toString()));
        return nodes;
    }

    @Override
    public void destroy() {
        try {
            if (serviceDiscovery != null) {
                serviceDiscovery.unregisterService(serviceInstance);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot unregister service: " + serviceInstance.getName(), e);
        }
    }

    private ServiceInstance<Void> buildServiceInstance(Address address) throws Exception {
        return ServiceInstance.<Void>builder()
                .uriSpec(new UriSpec("{scheme}://{address}:{port}"))
                .address(address.getHost())
                .port(address.getPort())
                .name(group)
                .build();
    }

    private SimpleDiscoveryNode prepareDiscoveryNode(ServiceInstance<Void> instance) {
        String host = instance.getAddress();
        Integer port = instance.getPort();
        try {
            return new SimpleDiscoveryNode(new Address(host, port));
        } catch (UnknownHostException e) {
            throw new IllegalStateException(String.format("Unknown node address: %s:%d ", host, port), e);
        }
    }
}
