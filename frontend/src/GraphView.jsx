import React, { useEffect, useRef } from "react";
import { Network } from "vis-network/standalone";

const GraphView = ({ data, showLabels, onNodeClick, onNodeHover }) => {
  const containerRef = useRef(null);
  const networkRef = useRef(null);

  // Toggle labels without destroying the network
  useEffect(() => {
    if (!networkRef.current) return;
    networkRef.current.setOptions({
      nodes: {
        font: { size: showLabels ? 11 : 0, color: "#374151" },
      },
    });
  }, [showLabels]);

  useEffect(() => {
    if (!containerRef.current || !data?.nodes?.length) return;

    if (networkRef.current) {
      networkRef.current.destroy();
      networkRef.current = null;
    }

    const options = {
      nodes: {
        shape: "dot",
        size: 7,
        font: { size: showLabels ? 11 : 0, color: "#374151", face: "Inter, sans-serif" },
        borderWidth: 1.5,
        shadow: false,
      },
      edges: {
        color: { color: "#bfdbfe", highlight: "#93c5fd", hover: "#60a5fa" },
        width: 0.8,
        smooth: { type: "continuous" },
        arrows: { to: { enabled: false } },
      },
      interaction: {
        hover: true,
        zoomView: true,
        dragView: true,
        tooltipDelay: 60,
        hideEdgesOnDrag: true,
      },
      physics: {
        enabled: true,
        stabilization: { iterations: 250, fit: true },
        barnesHut: {
          gravitationalConstant: -5000,
          springLength: 100,
          springConstant: 0.04,
          damping: 0.2,
        },
      },
    };

    const network = new Network(
      containerRef.current,
      { nodes: data.nodes, edges: data.edges },
      options
    );

    network.once("stabilizationIterationsDone", () => {
      network.setOptions({ physics: { enabled: false } });
      network.fit({ animation: { duration: 700, easingFunction: "easeInOutQuad" } });
    });

    network.on("click", (params) => {
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0];
        const nodeData = data.nodes.find((n) => n.id === nodeId);
        network.focus(nodeId, {
          scale: 2,
          animation: { duration: 500, easingFunction: "easeInOutQuad" },
        });
        onNodeClick?.(nodeId, nodeData);
      } else {
        onNodeClick?.(null, null);
      }
    });

    network.on("hoverNode", (params) => {
      if (containerRef.current) containerRef.current.style.cursor = "pointer";
      const nodeData = data.nodes.find((n) => n.id === params.node);
      onNodeHover?.(params.node, nodeData);
    });

    network.on("blurNode", () => {
      if (containerRef.current) containerRef.current.style.cursor = "default";
      onNodeHover?.(null, null);
    });

    networkRef.current = network;

    return () => {
      network.destroy();
      networkRef.current = null;
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  return <div ref={containerRef} style={{ width: "100%", height: "100%" }} />;
};

export default GraphView;