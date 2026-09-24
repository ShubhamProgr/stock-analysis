"use client";

import { useEffect, useRef } from "react";
import * as THREE from "three";

/**
 * ThreeBackground – Full-screen 3D "stock goes up" animation:
 *  • A sweeping 3D price line ribbon rising from left to right
 *  • 3D candlestick columns along the chart
 *  • Glowing particles streaming upward along the price path
 *  • Volume bars on the grid floor
 *  • Depth-of-field grid with fog
 *  • Mouse-reactive parallax camera
 */
export default function ThreeBackground() {
  const mountRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!mountRef.current) return;
    const container = mountRef.current;

    // ── Renderer ──
    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(window.innerWidth, window.innerHeight);
    renderer.setClearColor(0x000000, 0);
    container.appendChild(renderer.domElement);

    // ── Scene ──
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x080a0e, 0.018);

    // ── Camera ──
    const camera = new THREE.PerspectiveCamera(
      55,
      window.innerWidth / window.innerHeight,
      0.1,
      500
    );
    camera.position.set(0, 6, 28);
    camera.lookAt(0, 2, 0);

    // ── Lights ──
    const ambientLight = new THREE.AmbientLight(0x1a2540, 0.6);
    scene.add(ambientLight);

    const mainLight = new THREE.DirectionalLight(0x7da0de, 0.8);
    mainLight.position.set(10, 20, 15);
    scene.add(mainLight);

    const greenGlow = new THREE.PointLight(0x35c15e, 3, 40);
    greenGlow.position.set(12, 8, 5);
    scene.add(greenGlow);

    const blueGlow = new THREE.PointLight(0x7da0de, 2, 50);
    blueGlow.position.set(-10, 4, 10);
    scene.add(blueGlow);

    const warmGlow = new THREE.PointLight(0xf0b429, 1.2, 35);
    warmGlow.position.set(5, -2, 8);
    scene.add(warmGlow);

    // ── Generate Stock Price Data (trending UP) ──
    const dataPoints = 80;
    const prices: number[] = [];
    let price = 2;
    for (let i = 0; i < dataPoints; i++) {
      // Strong uptrend with realistic pullbacks
      const trend = 0.06;
      const volatility = (Math.random() - 0.42) * 0.5;
      const momentum = Math.sin(i * 0.15) * 0.15;
      price += trend + volatility + momentum;
      price = Math.max(price, 0.5);
      prices.push(price);
    }
    // Normalize prices to a visual range
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    const rangeP = maxP - minP || 1;
    const normalizedPrices = prices.map(
      (p) => ((p - minP) / rangeP) * 10
    );

    const chartStartX = -18;
    const chartSpacing = 0.5;
    const gridY = -2;

    // ── Grid Plane ──
    const gridHelper = new THREE.GridHelper(80, 80, 0x111825, 0x0c1018);
    gridHelper.position.y = gridY;
    (gridHelper.material as THREE.Material).transparent = true;
    (gridHelper.material as THREE.Material).opacity = 0.35;
    scene.add(gridHelper);

    // Subtle reflective floor plane
    const floorGeo = new THREE.PlaneGeometry(80, 80);
    const floorMat = new THREE.MeshPhongMaterial({
      color: 0x080a0e,
      transparent: true,
      opacity: 0.6,
      shininess: 80,
      specular: 0x1a2540,
    });
    const floor = new THREE.Mesh(floorGeo, floorMat);
    floor.rotation.x = -Math.PI / 2;
    floor.position.y = gridY - 0.01;
    scene.add(floor);

    // ── Price Line (Glowing 3D Tube) ──
    const lineCurvePoints: THREE.Vector3[] = [];
    for (let i = 0; i < dataPoints; i++) {
      lineCurvePoints.push(
        new THREE.Vector3(
          chartStartX + i * chartSpacing,
          gridY + 0.5 + normalizedPrices[i],
          0
        )
      );
    }
    const priceCurve = new THREE.CatmullRomCurve3(lineCurvePoints);

    // Main glowing line tube
    const tubeGeo = new THREE.TubeGeometry(priceCurve, 200, 0.06, 8, false);
    const tubeMat = new THREE.MeshPhongMaterial({
      color: 0x35c15e,
      emissive: 0x35c15e,
      emissiveIntensity: 0.8,
      transparent: true,
      opacity: 0.95,
      shininess: 100,
    });
    const priceLine = new THREE.Mesh(tubeGeo, tubeMat);
    scene.add(priceLine);

    // Wider glow aura around the line
    const glowTubeGeo = new THREE.TubeGeometry(priceCurve, 200, 0.25, 8, false);
    const glowTubeMat = new THREE.MeshBasicMaterial({
      color: 0x35c15e,
      transparent: true,
      opacity: 0.08,
    });
    const glowTube = new THREE.Mesh(glowTubeGeo, glowTubeMat);
    scene.add(glowTube);

    // Even wider ambient glow
    const auraGeo = new THREE.TubeGeometry(priceCurve, 200, 0.7, 8, false);
    const auraMat = new THREE.MeshBasicMaterial({
      color: 0x35c15e,
      transparent: true,
      opacity: 0.03,
    });
    const aura = new THREE.Mesh(auraGeo, auraMat);
    scene.add(aura);

    // ── Area Fill (translucent sheet from line to floor) ──
    const areaVertices: number[] = [];
    const areaIndices: number[] = [];
    const curveDetail = 200;
    const curvePoints = priceCurve.getPoints(curveDetail);

    for (let i = 0; i <= curveDetail; i++) {
      const pt = curvePoints[i];
      // Top point (on the line)
      areaVertices.push(pt.x, pt.y, pt.z);
      // Bottom point (on the floor)
      areaVertices.push(pt.x, gridY, pt.z);
    }
    for (let i = 0; i < curveDetail; i++) {
      const topLeft = i * 2;
      const bottomLeft = i * 2 + 1;
      const topRight = (i + 1) * 2;
      const bottomRight = (i + 1) * 2 + 1;
      areaIndices.push(topLeft, bottomLeft, topRight);
      areaIndices.push(bottomLeft, bottomRight, topRight);
    }
    const areaGeo = new THREE.BufferGeometry();
    areaGeo.setAttribute(
      "position",
      new THREE.Float32BufferAttribute(areaVertices, 3)
    );
    areaGeo.setIndex(areaIndices);
    areaGeo.computeVertexNormals();

    const areaMat = new THREE.ShaderMaterial({
      transparent: true,
      side: THREE.DoubleSide,
      depthWrite: false,
      uniforms: {
        uTopColor: { value: new THREE.Color(0x35c15e) },
        uBottomColor: { value: new THREE.Color(0x0a0c10) },
        uOpacity: { value: 0.15 },
        uGridY: { value: gridY },
        uMaxY: { value: gridY + 12 },
      },
      vertexShader: `
        varying float vY;
        void main() {
          vY = position.y;
          gl_Position = projectionMatrix * modelViewMatrix * vec4(position, 1.0);
        }
      `,
      fragmentShader: `
        uniform vec3 uTopColor;
        uniform vec3 uBottomColor;
        uniform float uOpacity;
        uniform float uGridY;
        uniform float uMaxY;
        varying float vY;
        void main() {
          float t = clamp((vY - uGridY) / (uMaxY - uGridY), 0.0, 1.0);
          vec3 col = mix(uBottomColor, uTopColor, t);
          float alpha = uOpacity * t;
          gl_FragColor = vec4(col, alpha);
        }
      `,
    });
    const areaFill = new THREE.Mesh(areaGeo, areaMat);
    scene.add(areaFill);

    // ── 3D Candlestick Columns ──
    const candlestickGroup = new THREE.Group();
    const candleInterval = 4; // every N data points
    for (let i = 0; i < dataPoints - 1; i += candleInterval) {
      const open = normalizedPrices[i];
      const close = normalizedPrices[Math.min(i + candleInterval - 1, dataPoints - 1)];
      const high = Math.max(
        ...normalizedPrices.slice(i, Math.min(i + candleInterval, dataPoints))
      );
      const low = Math.min(
        ...normalizedPrices.slice(i, Math.min(i + candleInterval, dataPoints))
      );
      const isGreen = close >= open;
      const bodyBot = Math.min(open, close);
      const bodyTop = Math.max(open, close);
      const bodyH = Math.max(bodyTop - bodyBot, 0.1);
      const x = chartStartX + (i + candleInterval / 2) * chartSpacing;

      // Candle body
      const bodyGeo = new THREE.BoxGeometry(
        chartSpacing * candleInterval * 0.55,
        bodyH,
        chartSpacing * candleInterval * 0.55
      );
      const bodyMat = new THREE.MeshPhongMaterial({
        color: isGreen ? 0x35c15e : 0xe8635f,
        transparent: true,
        opacity: 0.35,
        emissive: isGreen ? 0x35c15e : 0xe8635f,
        emissiveIntensity: 0.2,
        shininess: 60,
      });
      const body = new THREE.Mesh(bodyGeo, bodyMat);
      body.position.set(x, gridY + 0.5 + bodyBot + bodyH / 2, 0);
      candlestickGroup.add(body);

      // Candle wick
      const wickH = high - low;
      const wickGeo = new THREE.BoxGeometry(0.04, wickH, 0.04);
      const wickMat = new THREE.MeshBasicMaterial({
        color: isGreen ? 0x35c15e : 0xe8635f,
        transparent: true,
        opacity: 0.5,
      });
      const wick = new THREE.Mesh(wickGeo, wickMat);
      wick.position.set(x, gridY + 0.5 + low + wickH / 2, 0);
      candlestickGroup.add(wick);

      // Candle glow on floor
      const glowGeo = new THREE.PlaneGeometry(
        chartSpacing * candleInterval * 0.7,
        chartSpacing * candleInterval * 0.7
      );
      const glowMat = new THREE.MeshBasicMaterial({
        color: isGreen ? 0x35c15e : 0xe8635f,
        transparent: true,
        opacity: 0.04,
      });
      const glowPlane = new THREE.Mesh(glowGeo, glowMat);
      glowPlane.rotation.x = -Math.PI / 2;
      glowPlane.position.set(x, gridY + 0.01, 0);
      candlestickGroup.add(glowPlane);
    }
    scene.add(candlestickGroup);

    // ── Volume Bars on the Floor ──
    const volumeGroup = new THREE.Group();
    for (let i = 0; i < dataPoints - 1; i += candleInterval) {
      const vol = 0.3 + Math.random() * 1.2;
      const x = chartStartX + (i + candleInterval / 2) * chartSpacing;
      const isGreen =
        normalizedPrices[Math.min(i + candleInterval - 1, dataPoints - 1)] >=
        normalizedPrices[i];
      const barGeo = new THREE.BoxGeometry(
        chartSpacing * candleInterval * 0.4,
        vol,
        chartSpacing * candleInterval * 0.4
      );
      const barMat = new THREE.MeshPhongMaterial({
        color: isGreen ? 0x35c15e : 0xe8635f,
        transparent: true,
        opacity: 0.12,
        emissive: isGreen ? 0x35c15e : 0xe8635f,
        emissiveIntensity: 0.1,
      });
      const bar = new THREE.Mesh(barGeo, barMat);
      bar.position.set(x, gridY + vol / 2, 3.5);
      volumeGroup.add(bar);
    }
    scene.add(volumeGroup);

    // ── Streaming Particles Along the Price Line (flowing upward) ──
    const particleCount = 300;
    const particlePositions = new Float32Array(particleCount * 3);
    const particleSizes = new Float32Array(particleCount);
    const particleColors = new Float32Array(particleCount * 3);
    const particleOffsets = new Float32Array(particleCount); // t offset [0,1]

    const green = new THREE.Color(0x35c15e);
    const blue = new THREE.Color(0x7da0de);
    const gold = new THREE.Color(0xf0b429);
    const white = new THREE.Color(0xf2f1ec);

    for (let i = 0; i < particleCount; i++) {
      particleOffsets[i] = Math.random();
      particleSizes[i] = Math.random() * 4 + 1;
      // 70% green, 15% blue, 10% gold, 5% white
      const roll = Math.random();
      const col = roll < 0.7 ? green : roll < 0.85 ? blue : roll < 0.95 ? gold : white;
      particleColors[i * 3] = col.r;
      particleColors[i * 3 + 1] = col.g;
      particleColors[i * 3 + 2] = col.b;
    }

    const particleGeo = new THREE.BufferGeometry();
    particleGeo.setAttribute(
      "position",
      new THREE.BufferAttribute(particlePositions, 3)
    );
    particleGeo.setAttribute(
      "size",
      new THREE.BufferAttribute(particleSizes, 1)
    );
    particleGeo.setAttribute(
      "color",
      new THREE.BufferAttribute(particleColors, 3)
    );

    const particleVertexShader = `
      attribute float size;
      varying vec3 vColor;
      void main() {
        vColor = color;
        vec4 mvPosition = modelViewMatrix * vec4(position, 1.0);
        gl_PointSize = size * (200.0 / -mvPosition.z);
        gl_Position = projectionMatrix * mvPosition;
      }
    `;
    const particleFragmentShader = `
      varying vec3 vColor;
      void main() {
        float dist = length(gl_PointCoord - vec2(0.5));
        if (dist > 0.5) discard;
        float alpha = 1.0 - smoothstep(0.0, 0.5, dist);
        gl_FragColor = vec4(vColor, alpha * 0.65);
      }
    `;

    const particleMat = new THREE.ShaderMaterial({
      vertexShader: particleVertexShader,
      fragmentShader: particleFragmentShader,
      transparent: true,
      vertexColors: true,
      depthWrite: false,
      blending: THREE.AdditiveBlending,
    });
    const particles = new THREE.Points(particleGeo, particleMat);
    scene.add(particles);

    // ── Rising Spark Particles (ambient upward drift) ──
    const sparkCount = 400;
    const sparkPositions = new Float32Array(sparkCount * 3);
    const sparkVelocities: { x: number; y: number; z: number }[] = [];
    for (let i = 0; i < sparkCount; i++) {
      sparkPositions[i * 3] = (Math.random() - 0.5) * 50;
      sparkPositions[i * 3 + 1] = gridY + Math.random() * 20;
      sparkPositions[i * 3 + 2] = (Math.random() - 0.5) * 30;
      sparkVelocities.push({
        x: (Math.random() - 0.5) * 0.003,
        y: 0.005 + Math.random() * 0.015,
        z: (Math.random() - 0.5) * 0.003,
      });
    }
    const sparkGeo = new THREE.BufferGeometry();
    sparkGeo.setAttribute(
      "position",
      new THREE.BufferAttribute(sparkPositions, 3)
    );
    const sparkMat = new THREE.PointsMaterial({
      color: 0x35c15e,
      size: 0.06,
      transparent: true,
      opacity: 0.35,
      blending: THREE.AdditiveBlending,
      depthWrite: false,
    });
    const sparks = new THREE.Points(sparkGeo, sparkMat);
    scene.add(sparks);

    // ── Horizontal Price Grid Lines (floating in space) ──
    const priceLines: THREE.Line[] = [];
    for (let p = 0; p <= 10; p += 2) {
      const y = gridY + 0.5 + p;
      const points = [
        new THREE.Vector3(chartStartX - 2, y, -2),
        new THREE.Vector3(chartStartX + dataPoints * chartSpacing + 2, y, -2),
      ];
      const lineGeo = new THREE.BufferGeometry().setFromPoints(points);
      const lineMat = new THREE.LineBasicMaterial({
        color: 0x7da0de,
        transparent: true,
        opacity: 0.05,
      });
      const line = new THREE.Line(lineGeo, lineMat);
      scene.add(line);
      priceLines.push(line);
    }

    // ── "UP" Arrow / Trend Indicator at the end of the chart ──
    const arrowGroup = new THREE.Group();
    const lastPrice = normalizedPrices[normalizedPrices.length - 1];
    const arrowX = chartStartX + (dataPoints - 1) * chartSpacing + 1;
    const arrowY = gridY + 0.5 + lastPrice;

    // Arrow shaft
    const shaftGeo = new THREE.CylinderGeometry(0.08, 0.08, 2.5, 8);
    const shaftMat = new THREE.MeshPhongMaterial({
      color: 0x35c15e,
      emissive: 0x35c15e,
      emissiveIntensity: 0.6,
      transparent: true,
      opacity: 0.8,
    });
    const shaft = new THREE.Mesh(shaftGeo, shaftMat);
    shaft.position.set(arrowX, arrowY + 1.25, 0);
    arrowGroup.add(shaft);

    // Arrow head
    const headGeo = new THREE.ConeGeometry(0.35, 0.8, 8);
    const headMat = new THREE.MeshPhongMaterial({
      color: 0x35c15e,
      emissive: 0x35c15e,
      emissiveIntensity: 0.8,
      transparent: true,
      opacity: 0.9,
    });
    const head = new THREE.Mesh(headGeo, headMat);
    head.position.set(arrowX, arrowY + 2.9, 0);
    arrowGroup.add(head);

    // Arrow glow
    const arrowGlowGeo = new THREE.SphereGeometry(1.2, 16, 16);
    const arrowGlowMat = new THREE.MeshBasicMaterial({
      color: 0x35c15e,
      transparent: true,
      opacity: 0.06,
    });
    const arrowGlowMesh = new THREE.Mesh(arrowGlowGeo, arrowGlowMat);
    arrowGlowMesh.position.set(arrowX, arrowY + 2, 0);
    arrowGroup.add(arrowGlowMesh);

    scene.add(arrowGroup);

    // ── Pulsing Beacon at the Chart Tip ──
    const beaconGeo = new THREE.SphereGeometry(0.15, 16, 16);
    const beaconMat = new THREE.MeshBasicMaterial({
      color: 0x35c15e,
    });
    const beacon = new THREE.Mesh(beaconGeo, beaconMat);
    const lastPt = lineCurvePoints[lineCurvePoints.length - 1];
    beacon.position.copy(lastPt);
    scene.add(beacon);

    const beaconRingGeo = new THREE.RingGeometry(0.2, 0.5, 32);
    const beaconRingMat = new THREE.MeshBasicMaterial({
      color: 0x35c15e,
      transparent: true,
      opacity: 0.4,
      side: THREE.DoubleSide,
    });
    const beaconRing = new THREE.Mesh(beaconRingGeo, beaconRingMat);
    beaconRing.position.copy(lastPt);
    scene.add(beaconRing);

    // ── Mouse tracking ──
    const mouse = { x: 0, y: 0 };
    const handleMouseMove = (e: MouseEvent) => {
      mouse.x = (e.clientX / window.innerWidth - 0.5) * 2;
      mouse.y = (e.clientY / window.innerHeight - 0.5) * 2;
    };
    window.addEventListener("mousemove", handleMouseMove);

    // ── Resize ──
    const handleResize = () => {
      camera.aspect = window.innerWidth / window.innerHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(window.innerWidth, window.innerHeight);
    };
    window.addEventListener("resize", handleResize);

    // ── Animation Loop ──
    let animId: number;
    const startTime = performance.now();

    const animate = () => {
      animId = requestAnimationFrame(animate);
      const elapsed = (performance.now() - startTime) / 1000;

      // ── Streaming Particles along the price curve ──
      const positions = particleGeo.attributes.position.array as Float32Array;
      for (let i = 0; i < particleCount; i++) {
        // Each particle moves along the curve
        let t = (particleOffsets[i] + elapsed * (0.03 + (i % 5) * 0.008)) % 1;
        const pt = priceCurve.getPointAt(t);

        // Add slight random scatter around the line
        const scatter = Math.sin(elapsed * 3 + i) * 0.3;
        const scatterZ = Math.cos(elapsed * 2 + i * 0.7) * 0.5;

        positions[i * 3] = pt.x + scatter * 0.3;
        positions[i * 3 + 1] = pt.y + Math.abs(scatter) * 0.5; // drift upward
        positions[i * 3 + 2] = pt.z + scatterZ;
      }
      particleGeo.attributes.position.needsUpdate = true;

      // Pulse particle sizes
      const sizes = particleGeo.attributes.size.array as Float32Array;
      for (let i = 0; i < particleCount; i++) {
        sizes[i] = (Math.sin(elapsed * 4 + i * 0.3) * 0.5 + 1.5) * (i % 4 === 0 ? 2 : 1.2);
      }
      particleGeo.attributes.size.needsUpdate = true;

      // ── Rising Spark Particles ──
      const sp = sparkGeo.attributes.position.array as Float32Array;
      for (let i = 0; i < sparkCount; i++) {
        sp[i * 3] += sparkVelocities[i].x;
        sp[i * 3 + 1] += sparkVelocities[i].y;
        sp[i * 3 + 2] += sparkVelocities[i].z;
        // Reset when too high
        if (sp[i * 3 + 1] > 20) {
          sp[i * 3] = (Math.random() - 0.5) * 50;
          sp[i * 3 + 1] = gridY;
          sp[i * 3 + 2] = (Math.random() - 0.5) * 30;
        }
      }
      sparkGeo.attributes.position.needsUpdate = true;

      // ── Pulsing Beacon ──
      const beaconScale = 1 + Math.sin(elapsed * 3) * 0.3;
      beacon.scale.setScalar(beaconScale);
      const ringScale = 1 + Math.sin(elapsed * 2) * 0.5;
      beaconRing.scale.setScalar(ringScale);
      beaconRingMat.opacity = 0.4 - Math.sin(elapsed * 2) * 0.25;
      beaconRing.lookAt(camera.position);

      // ── Arrow Bob ──
      arrowGroup.position.y = Math.sin(elapsed * 1.5) * 0.3;
      arrowGlowMat.opacity = 0.04 + Math.sin(elapsed * 2) * 0.03;

      // ── Candlestick subtle pulse ──
      candlestickGroup.children.forEach((child, idx) => {
        if (child instanceof THREE.Mesh && child.geometry instanceof THREE.BoxGeometry) {
          const mat = child.material as THREE.MeshPhongMaterial;
          if (mat.emissiveIntensity !== undefined) {
            mat.emissiveIntensity = 0.15 + Math.sin(elapsed * 2 + idx * 0.3) * 0.1;
          }
        }
      });

      // ── Light pulses ──
      greenGlow.intensity = 3 + Math.sin(elapsed * 1.5) * 0.8;
      blueGlow.intensity = 2 + Math.sin(elapsed * 1.2 + 1) * 0.5;
      warmGlow.intensity = 1.2 + Math.sin(elapsed * 0.9 + 2) * 0.4;

      // ── Camera follows mouse + slight orbit ──
      const targetX = mouse.x * 3 + Math.sin(elapsed * 0.15) * 2;
      const targetY = -mouse.y * 2 + 6 + Math.sin(elapsed * 0.2) * 0.5;
      camera.position.x += (targetX - camera.position.x) * 0.015;
      camera.position.y += (targetY - camera.position.y) * 0.015;
      camera.lookAt(0, 2, 0);

      renderer.render(scene, camera);
    };

    animate();

    return () => {
      cancelAnimationFrame(animId);
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("resize", handleResize);
      renderer.dispose();
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, []);

  return (
    <div
      ref={mountRef}
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        width: "100%",
        height: "100%",
        zIndex: 0,
        pointerEvents: "none",
      }}
    />
  );
}
