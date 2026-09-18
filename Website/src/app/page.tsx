"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";

const CHART_PRICES = [412,418,415,422,430,428,435,448,444,452,458,455,462,470,465,472,480,478,485,492,488,496,502,498,508,514,510,518,524,520,528,535,530,538,544,540,548,555,550,558,564,560,568,575,570,578,584,580,588,595];
const SENTIMENT_DATA = [0.2,0.4,0.35,0.55,0.6,0.5,0.7,0.8,0.75,0.65,0.85,0.9];

function buildSvgPath(data: number[], w: number, h: number, pad = 8) {
  const mn = Math.min(...data), mx = Math.max(...data), range = mx - mn || 1;
  return "M" + data.map((v, i) => `${pad + (i / (data.length - 1)) * (w - pad * 2)},${pad + (1 - (v - mn) / range) * (h - pad * 2)}`).join("L");
}


const FEATURES = [
  {label:"Historic Stock Data",sub:"Interactive candlestick & line charts",desc:"Explore years of NSE price history with candlestick overlays, volume bars, and zoom controls. Filter by 1M-5Y ranges.",accent:"#7da0de",glow:"rgba(125,160,222,0.14)"},
  {label:"Sentiment Analysis",sub:"NLP-driven market mood scoring",desc:"News articles scraped daily, scored by fine-tuned NLP. Track market mood shifts alongside live price action.",accent:"#35c15e",glow:"rgba(53,193,94,0.14)"},
  {label:"XGBoost Predictions",sub:"Gradient-boosted ML forecasts",desc:"XGBoost trained on price, volume, 20+ indicators, and sentiment scores generates next-day directional signals.",accent:"#f0b429",glow:"rgba(240,180,41,0.14)"},
  {label:"Model Accuracy",sub:"Backtested performance visibility",desc:"Rolling accuracy, precision, and recall for every ticker. Prediction-vs-actual overlay across historical dates.",accent:"#e8635f",glow:"rgba(232,99,95,0.14)"},
  {label:"Market Overview",sub:"Sector heatmaps & top movers",desc:"Macro NSE view: sector heatmaps, top gainers/losers, and market breadth indicators.",accent:"#a78bfa",glow:"rgba(167,139,250,0.14)"},
  {label:"Compare & Screener",sub:"Multi-stock comparison tools",desc:"Compare NSE equities side-by-side. Screen by PE ratio, market cap, signal strength, and fundamentals.",accent:"#38bdf8",glow:"rgba(56,189,248,0.14)"},
];

const ICON_SVG_CONTENT: Record<number, string> = {
  0: '<path d="M3 19L8 11L13 14L20 5" stroke="#7da0de" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/><circle cx="20" cy="5" r="2.5" fill="#7da0de"/>',
  1: '<circle cx="13" cy="13" r="8" stroke="#35c15e" strokeWidth="1.8"/><path d="M8 16c0-2.8 2.2-4.8 5-2.8s4.5 0 4.5-2.8" stroke="#35c15e" strokeWidth="1.8" strokeLinecap="round"/>',
  2: '<rect x="3" y="15" width="4" height="8" rx="1.5" fill="#f0b429"/><rect x="10" y="9" width="4" height="14" rx="1.5" fill="#f0b429" opacity="0.8"/><rect x="17" y="3" width="4" height="20" rx="1.5" fill="#f0b429" opacity="0.6"/>',
  3: '<path d="M4 13L10 20L22 6" stroke="#e8635f" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"/>',
  4: '<rect x="3" y="3" width="8" height="8" rx="2" stroke="#a78bfa" strokeWidth="1.8"/><rect x="15" y="3" width="8" height="8" rx="2" stroke="#a78bfa" strokeWidth="1.8"/><rect x="3" y="15" width="8" height="8" rx="2" stroke="#a78bfa" strokeWidth="1.8"/><rect x="15" y="15" width="8" height="8" rx="2" stroke="#a78bfa" strokeWidth="1.8"/>',
  5: '<path d="M3 20L8 13L13 16L19 7L23 11" stroke="#38bdf8" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/><path d="M3 20L8 16L13 19L19 12L23 15" stroke="#38bdf8" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" opacity="0.4"/>',
};

const BG_BAR_BASE = 620;
const BG_BAR_W = 40;
const BG_BARS: { x: number; h: number }[] = [
  { x: 30, h: 50 }, { x: 95, h: 70 }, { x: 160, h: 58 }, { x: 225, h: 92 },
  { x: 290, h: 120 }, { x: 355, h: 106 }, { x: 420, h: 138 }, { x: 485, h: 164 },
  { x: 550, h: 148 }, { x: 615, h: 178 }, { x: 680, h: 202 }, { x: 745, h: 186 },
  { x: 810, h: 216 }, { x: 875, h: 240 }, { x: 940, h: 222 }, { x: 1005, h: 252 },
  { x: 1070, h: 274 }, { x: 1135, h: 258 }, { x: 1200, h: 288 }, { x: 1265, h: 312 },
  { x: 1330, h: 296 }, { x: 1390, h: 320 },
];
const BG_LINE_PATH = (() => {
  let d = "";
  BG_BARS.forEach((b, i) => { d += `${i === 0 ? "M" : "L"}${b.x},${BG_BAR_BASE - b.h}`; });
  return d;
})();
const BG_LINE_AREA_PATH = BG_LINE_PATH + " L1430,780 L20,780 Z";

export default function LandingPage() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const animRef = useRef<number>(0);
  const particlesRef = useRef<{x:number;y:number;vx:number;vy:number;r:number;opacity:number}[]>([]);
  const [chartProgress, setChartProgress] = useState(0);
  const [mounted, setMounted] = useState(false);
  const [hovered, setHovered] = useState<number|null>(null);

  useEffect(() => {
    setMounted(true);
    particlesRef.current = Array.from({length:55}, () => ({
      x:Math.random()*window.innerWidth, y:Math.random()*window.innerHeight,
      vx:(Math.random()-0.5)*0.3, vy:-Math.random()*0.5-0.15,
      r:Math.random()*1.8+0.4, opacity:Math.random()*0.5+0.1,
    }));
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d")!;
    let W = window.innerWidth, H = window.innerHeight;
    canvas.width = W; canvas.height = H;
    const onResize = () => { W=window.innerWidth; H=window.innerHeight; canvas.width=W; canvas.height=H; };
    window.addEventListener("resize", onResize);
    const draw = () => {
      ctx.clearRect(0,0,W,H);
      const ps = particlesRef.current;
      for (const p of ps) {
        p.x+=p.vx; p.y+=p.vy;
        if(p.x<0)p.x=W; if(p.x>W)p.x=0;
        if(p.y<0){p.y=H; p.x=Math.random()*W;}
        if(p.y>H)p.y=0;
        ctx.beginPath(); ctx.arc(p.x,p.y,p.r,0,Math.PI*2);
        ctx.fillStyle=`rgba(125,160,222,${p.opacity})`; ctx.fill();
      }
      for(let i=0;i<ps.length;i++){for(let j=i+1;j<ps.length;j++){
        const dx=ps[i].x-ps[j].x,dy=ps[i].y-ps[j].y,d=Math.sqrt(dx*dx+dy*dy);
        if(d<110){ctx.beginPath();ctx.moveTo(ps[i].x,ps[i].y);ctx.lineTo(ps[j].x,ps[j].y);
          ctx.strokeStyle=`rgba(125,160,222,${(1-d/110)*0.12})`;ctx.lineWidth=0.6;ctx.stroke();}
      }}
      animRef.current=requestAnimationFrame(draw);
    };
    draw();
    let frame=0, start:number|null=null;
    const animChart=(ts:number)=>{if(!start)start=ts;const el=ts-start;setChartProgress(Math.min(el/2200,1));if(el<2200)frame=requestAnimationFrame(animChart);};
    const delay=setTimeout(()=>{frame=requestAnimationFrame(animChart);},600);
    return()=>{cancelAnimationFrame(animRef.current);cancelAnimationFrame(frame);clearTimeout(delay);window.removeEventListener("resize",onResize);};
  }, []);

  const CW=680, CH=180;
  const vCount=Math.max(2,Math.floor(chartProgress*CHART_PRICES.length));
  const partialPath=buildSvgPath(CHART_PRICES.slice(0,vCount),CW,CH);
  const sentProg=Math.max(0,chartProgress*1.4-0.4);

  const renderEndDot = () => {
    if(chartProgress<=0.1)return null;
    const idx=vCount-1, mn=Math.min(...CHART_PRICES), mx=Math.max(...CHART_PRICES);
    const ex=8+(idx/(CHART_PRICES.length-1))*(CW-16);
    const ey=8+(1-(CHART_PRICES[idx]-mn)/(mx-mn))*(CH-16);
    return <g><circle cx={ex} cy={ey} r="8" fill="rgba(125,160,222,0.15)"/><circle cx={ex} cy={ey} r="3" fill="#7da0de"/></g>;
  };

  const PRED_ROWS = [
    {label:"Yesterday",actual:2918,predicted:2895},
    {label:"2 days ago",actual:2884,predicted:2901},
    {label:"3 days ago",actual:2872,predicted:2866},
    {label:"4 days ago",actual:2851,predicted:2838},
    {label:"5 days ago",actual:2836,predicted:2850},
  ];

  return (
    <div style={{minHeight:"100vh",background:"#0d0f13",color:"#f2f1ec",fontFamily:"'Inter','Helvetica Neue',Arial,sans-serif",overflowX:"hidden",position:"relative"}}>

      {/* Canvas particles */}
      <canvas ref={canvasRef} style={{position:"fixed",top:0,left:0,width:"100%",height:"100%",pointerEvents:"none",zIndex:0}}/>
      {/* Glowing orbs */}
      <div style={{position:"fixed",top:"-15%",left:"-5%",width:750,height:750,borderRadius:"50%",background:"radial-gradient(circle,rgba(59,91,165,0.18) 0%,transparent 70%)",pointerEvents:"none",zIndex:0,filter:"blur(60px)"}}/>
      <div style={{position:"fixed",top:"25%",right:"-10%",width:650,height:650,borderRadius:"50%",background:"radial-gradient(circle,rgba(53,193,94,0.09) 0%,transparent 70%)",pointerEvents:"none",zIndex:0,filter:"blur(70px)"}}/>
      <div style={{position:"fixed",bottom:"15%",left:"25%",width:550,height:550,borderRadius:"50%",background:"radial-gradient(circle,rgba(59,91,165,0.08) 0%,transparent 70%)",pointerEvents:"none",zIndex:0,filter:"blur(70px)"}}/>

      {/* ── NAV ── */}
      <nav style={{position:"fixed",top:0,left:0,right:0,zIndex:100,background:"rgba(13,15,19,0.85)",backdropFilter:"blur(20px)",WebkitBackdropFilter:"blur(20px)",borderBottom:"1px solid rgba(242,241,236,0.08)"}}>
        <div style={{maxWidth:1240,margin:"0 auto",padding:"14px 32px",display:"flex",alignItems:"center",justifyContent:"space-between"}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <div style={{width:30,height:30,borderRadius:8,background:"linear-gradient(135deg,#3b5ba5,#7da0de)",display:"flex",alignItems:"center",justifyContent:"center",boxShadow:"0 0 16px rgba(125,160,222,0.35)"}}>
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 12L6 7L9 9.5L14 3" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
            </div>
            <span style={{fontWeight:700,fontSize:15,letterSpacing:"-0.01em",color:"#f2f1ec"}}>Stock Analytics</span>
            <span style={{fontSize:11,color:"#8b897f"}}>by Shubham</span>
          </div>
          <Link href="/dashboard" style={{display:"inline-flex",alignItems:"center",gap:6,background:"linear-gradient(135deg,#3b5ba5,#7da0de)",color:"#fff",borderRadius:8,padding:"8px 18px",fontSize:13,fontWeight:600,textDecoration:"none",border:"1px solid rgba(255,255,255,0.18)",boxShadow:"0 0 20px rgba(125,160,222,0.3)"}}>Go to Dashboard &rarr;</Link>
        </div>
      </nav>

      {/* ── HERO (UPSCALING DESIGN) ── */}
      <section style={{position:"relative",zIndex:10,maxWidth:1280,margin:"0 auto",padding:"140px 24px 84px",display:"flex",flexDirection:"column",alignItems:"center",textAlign:"center"}}>

        {/* ── BACKGROUND ANIMATED STOCK GRAPH (GOING UP) ── */}
        <div style={{position:"absolute",top:0,left:"-5%",right:"-5%",bottom:0,overflow:"hidden",pointerEvents:"none",zIndex:0}}>
          <svg viewBox="0 0 1440 780" preserveAspectRatio="none" style={{width:"100%",height:"100%",display:"block",opacity:0.92}}>
            <defs>
              <linearGradient id="bg-stock-line-grad" x1="0%" y1="100%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#3b5ba5" stopOpacity="0.3"/>
                <stop offset="35%" stopColor="#7da0de" stopOpacity="0.85"/>
                <stop offset="80%" stopColor="#35c15e" stopOpacity="0.95"/>
                <stop offset="100%" stopColor="#35c15e" stopOpacity="1"/>
              </linearGradient>
              <linearGradient id="bg-stock-area-grad" x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor="#35c15e" stopOpacity="0.16"/>
                <stop offset="45%" stopColor="#7da0de" stopOpacity="0.05"/>
                <stop offset="90%" stopColor="#0d0f13" stopOpacity="0"/>
              </linearGradient>
              <linearGradient id="bg-bar-grad" x1="0%" y1="0%" x2="100%" y2="0%">
                <stop offset="0%" stopColor="#3b5ba5"/>
                <stop offset="50%" stopColor="#7da0de"/>
                <stop offset="100%" stopColor="#35c15e"/>
              </linearGradient>
              <filter id="stock-glow" x="-20%" y="-20%" width="140%" height="140%">
                <feGaussianBlur stdDeviation="7" result="blur"/>
                <feMerge>
                  <feMergeNode in="blur"/>
                  <feMergeNode in="blur"/>
                  <feMergeNode in="SourceGraphic"/>
                </feMerge>
              </filter>
            </defs>

            {/* Financial Grid Lines */}
            {[
              {y:130,label:"₹3,450.00"},
              {y:270,label:"₹3,100.00"},
              {y:410,label:"₹2,750.00"},
              {y:550,label:"₹2,400.00"},
              {y:690,label:"₹2,050.00"},
            ].map((grid,i)=>(
              <g key={i}>
                <line x1="0" y1={grid.y} x2="1440" y2={grid.y} stroke="rgba(242,241,236,0.04)" strokeWidth="1" strokeDasharray="6 6"/>
                <text x="1420" y={grid.y-6} fill="rgba(139,137,127,0.35)" fontSize="10.5" fontFamily="IBM Plex Mono, monospace" textAnchor="end">{grid.label}</text>
              </g>
            ))}

            {[180,420,680,940,1200].map((vx,i)=>(
              <line key={i} x1={vx} y1="60" x2={vx} y2="740" stroke="rgba(242,241,236,0.03)" strokeWidth="1" strokeDasharray="4 8"/>
            ))}

            {/* Rising Bar Chart */}
            {BG_BARS.map((b,i)=>(
              <g key={i} style={{transformOrigin:`${b.x+BG_BAR_W/2}px ${BG_BAR_BASE}px`,animation:`stock-bar-rise 0.6s cubic-bezier(.22,.68,0,1.2) ${0.08+i*0.05}s both`}}>
                <rect x={b.x} y={BG_BAR_BASE-b.h} width={BG_BAR_W} height={b.h} rx="4" fill="url(#bg-bar-grad)"/>
                <line x1={b.x} y1={BG_BAR_BASE-b.h} x2={b.x+BG_BAR_W} y2={BG_BAR_BASE-b.h} stroke="rgba(255,255,255,0.4)" strokeWidth="1" style={{animation:`stock-bar-rise 0.6s cubic-bezier(.22,.68,0,1.2) ${0.08+i*0.05}s both`}}/>
              </g>
            ))}

            {/* Area Fill Under Rising Line */}
            <path d={BG_LINE_AREA_PATH} fill="url(#bg-stock-area-grad)"/>

            {/* Glowing Neon Line */}
            <path d={BG_LINE_PATH} fill="none" stroke="rgba(125,160,222,0.35)" strokeWidth="10" filter="url(#stock-glow)"/>

            {/* Main Rising Line tracing over bar tops (animated drawing) */}
            <path d={BG_LINE_PATH} fill="none" stroke="url(#bg-stock-line-grad)" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" style={{strokeDasharray:2200,strokeDashoffset:2200,animation:"stock-line-draw 1.8s cubic-bezier(0.16,1,0.3,1) 0.2s forwards"}}/>

            {/* Live Electric Flowing Pulse Dash */}
            <path d={BG_LINE_PATH} fill="none" stroke="rgba(242,241,236,0.85)" strokeWidth="2.2" strokeLinecap="round" strokeDasharray="18 160" style={{animation:"stock-flow 3.2s linear infinite"}}/>

            {/* Pulsing Radar Beacon at the Peak */}
            <g transform="translate(1400, 300)">
              <circle cx="0" cy="0" r="14" fill="none" stroke="#7da0de" style={{animation:"stock-radar 2.2s cubic-bezier(0,0.2,0.8,1) infinite"}}/>
              <circle cx="0" cy="0" r="14" fill="none" stroke="#35c15e" style={{animation:"stock-radar 2.2s cubic-bezier(0,0.2,0.8,1) infinite 0.7s"}}/>
              <circle cx="0" cy="0" r="7" fill="#35c15e"/>
              <circle cx="0" cy="0" r="3" fill="#f2f1ec"/>
            </g>
          </svg>

          {/* Floating Milestone Badges */}
          <div style={{position:"absolute",left:"10%",top:"72%",transform:"translate(-50%,-50%)",display:"inline-flex",alignItems:"center",gap:6,padding:"6px 12px",background:"rgba(23,27,33,0.85)",backdropFilter:"blur(16px)",border:"1px solid rgba(242,241,236,0.1)",borderRadius:20,fontSize:11,fontFamily:"IBM Plex Mono, monospace",color:"#7da0de",boxShadow:"0 12px 30px rgba(0,0,0,0.5)",animation:"stock-float 4.5s ease-in-out infinite"}}>
            <span style={{width:6,height:6,borderRadius:"50%",background:"#7da0de",boxShadow:"0 0 6px #7da0de"}}/>
            BREAKOUT CONFIRMED &middot; VOL 2.4x
          </div>

          <div style={{position:"absolute",left:"82%",top:"42%",transform:"translate(-50%,-50%)",display:"inline-flex",alignItems:"center",gap:6,padding:"6px 14px",background:"rgba(23,27,33,0.85)",backdropFilter:"blur(16px)",border:"1px solid rgba(53,193,94,0.25)",borderRadius:20,fontSize:11,fontFamily:"IBM Plex Mono, monospace",color:"#35c15e",boxShadow:"0 12px 30px rgba(0,0,0,0.5), 0 0 16px rgba(53,193,94,0.12)",animation:"stock-float 5s ease-in-out infinite 1.2s"}}>
            <span style={{width:6,height:6,borderRadius:"50%",background:"#35c15e",boxShadow:"0 0 8px #35c15e"}}/>
            XGBOOST ML: 84.3% BUY SIGNAL
          </div>

          <div style={{position:"absolute",right:"6%",top:"8%",display:"inline-flex",alignItems:"center",gap:7,padding:"7px 16px",background:"rgba(23,27,33,0.9)",backdropFilter:"blur(16px)",border:"1px solid rgba(53,193,94,0.35)",borderRadius:20,fontSize:12,fontWeight:700,fontFamily:"IBM Plex Mono, monospace",color:"#f2f1ec",boxShadow:"0 12px 32px rgba(0,0,0,0.6), 0 0 24px rgba(53,193,94,0.18)",animation:"stock-float 4.2s ease-in-out infinite 0.6s"}}>
            <span style={{color:"#35c15e",fontSize:13}}>▲</span>
            NSE ALL-TIME HIGH &middot; <span style={{color:"#35c15e"}}>+32.4%</span>
          </div>
        </div>

        {/* ── CENTRALIZED HERO CONTENT ── */}
        <div style={{position:"relative",zIndex:2,maxWidth:880,margin:"0 auto",display:"flex",flexDirection:"column",alignItems:"center",textAlign:"center"}}>
          <div style={{display:"inline-flex",alignItems:"center",gap:8,background:"rgba(125,160,222,0.08)",border:"1px solid rgba(125,160,222,0.2)",borderRadius:100,padding:"6px 16px",fontSize:11.5,fontWeight:600,color:"#7da0de",letterSpacing:"0.06em",textTransform:"uppercase" as const,marginBottom:24,backdropFilter:"blur(8px)",boxShadow:"0 0 20px rgba(125,160,222,0.12)"}}>
            <span style={{width:7,height:7,borderRadius:"50%",background:"#35c15e",boxShadow:"0 0 10px #35c15e",display:"inline-block"}}/>
            Next-Gen Financial Intelligence &middot; NSE Live Data
          </div>
          <h1 style={{fontSize:"clamp(44px,6.2vw,74px)",fontWeight:800,lineHeight:1.06,letterSpacing:"-0.035em",color:"#f2f1ec",margin:"0 0 22px",textAlign:"center"}}>
            Intelligent Stock<br/>
            <span style={{background:"linear-gradient(135deg,#f2f1ec 0%,#7da0de 45%,#35c15e 90%)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",backgroundClip:"text",textShadow:"0 0 40px rgba(125,160,222,0.15)"}}>Analysis Platform</span>
          </h1>
          <p style={{fontSize:"clamp(16px,1.8vw,19px)",color:"#c3c2b7",lineHeight:1.65,margin:"0 auto 36px",maxWidth:660,textAlign:"center"}}>Harness gradient-boosted ML predictions, live sentiment scoring, and a vibrant data-rich dashboard to stay ahead of the NSE market.</p>
          <div style={{display:"flex",alignItems:"center",justifyContent:"center",gap:16,marginBottom:48,flexWrap:"wrap" as const}}>
            <Link href="/dashboard" style={{display:"inline-flex",alignItems:"center",gap:9,background:"linear-gradient(135deg,#3b5ba5 0%,#7da0de 100%)",color:"#fff",borderRadius:10,padding:"13px 28px",fontSize:14.5,fontWeight:700,textDecoration:"none",border:"1px solid rgba(255,255,255,0.2)",boxShadow:"0 0 32px rgba(125,160,222,0.35),0 8px 24px rgba(0,0,0,0.5)"}}>
              <svg width="17" height="17" viewBox="0 0 16 16" fill="none"><path d="M2 10L5 6L8 7.5L13 2" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
              Go to Dashboard &rarr;
            </Link>
            <a href="#features" style={{display:"inline-flex",alignItems:"center",background:"rgba(23,27,33,0.7)",color:"#f2f1ec",borderRadius:10,padding:"13px 24px",fontSize:14.5,fontWeight:600,textDecoration:"none",border:"1px solid rgba(242,241,236,0.14)",backdropFilter:"blur(14px)"}}>Explore Features &#8595;</a>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:0,maxWidth:840,width:"100%",background:"rgba(23,27,33,0.75)",backdropFilter:"blur(20px)",WebkitBackdropFilter:"blur(20px)",border:"1px solid rgba(242,241,236,0.08)",borderRadius:16,padding:"18px 0",boxShadow:"0 24px 60px -20px rgba(0,0,0,0.75)",marginBottom:48}}>
            {([{val:"500+",lbl:"NSE Equities"},{val:"5Y",lbl:"History Depth"},{val:"Daily",lbl:"Sentiment Refresh"},{val:"XGBoost",lbl:"ML Engine"}] as const).map((s,i)=>(
              <div key={i} style={{display:"flex",flexDirection:"column",alignItems:"center",gap:3,padding:"0 16px",borderRight:i<3?"1px solid rgba(242,241,236,0.08)":"none"}}>
                <span style={{fontSize:24,fontWeight:800,color:"#f2f1ec",letterSpacing:"-0.02em",fontFamily:"IBM Plex Mono,monospace"}}>{s.val}</span>
                <span style={{fontSize:10.5,color:"#8b897f",fontWeight:600,letterSpacing:"0.07em",textTransform:"uppercase" as const}}>{s.lbl}</span>
              </div>
            ))}
          </div>
        </div>

        {/* ── SHOWCASE TERMINAL CARD (UPSCALING DESIGN) ── */}
        <div style={{position:"relative",zIndex:2,width:"100%",maxWidth:760,background:"rgba(23,27,33,0.88)",backdropFilter:"blur(24px)",WebkitBackdropFilter:"blur(24px)",border:"1px solid rgba(242,241,236,0.12)",borderRadius:20,overflow:"hidden",boxShadow:"0 0 0 1px rgba(242,241,236,0.04),0 36px 80px -20px rgba(0,0,0,0.85),0 0 50px -15px rgba(125,160,222,0.12)",textAlign:"left" as const}}>
          {/* Terminal Titlebar */}
          <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"10px 18px",background:"rgba(17,21,28,0.75)",borderBottom:"1px solid rgba(242,241,236,0.08)"}}>
            <div style={{display:"flex",alignItems:"center",gap:6}}>
              <span style={{width:9,height:9,borderRadius:"50%",background:"#e8635f",display:"inline-block",opacity:0.8}}/>
              <span style={{width:9,height:9,borderRadius:"50%",background:"#f0b429",display:"inline-block",opacity:0.8}}/>
              <span style={{width:9,height:9,borderRadius:"50%",background:"#35c15e",display:"inline-block",opacity:0.8}}/>
              <span style={{marginLeft:10,fontSize:10.5,fontFamily:"IBM Plex Mono, monospace",color:"#8b897f",letterSpacing:"0.05em",textTransform:"uppercase" as const}}>NSE Live Feed &middot; Interactive Preview</span>
            </div>
            <div style={{display:"flex",alignItems:"center",gap:6,fontSize:10.5,fontFamily:"IBM Plex Mono, monospace",color:"#35c15e"}}>
              <span style={{width:6,height:6,borderRadius:"50%",background:"#35c15e",boxShadow:"0 0 6px #35c15e",display:"inline-block"}}/>
              MARKET OPEN
            </div>
          </div>

          <div style={{padding:"22px 24px 20px"}}>
            <div style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",marginBottom:14}}>
              <div>
                <div style={{fontFamily:"IBM Plex Mono,monospace",fontWeight:700,fontSize:14.5,color:"#f2f1ec",letterSpacing:"0.02em"}}>RELIANCE.NS</div>
                <div style={{fontSize:11.5,color:"#8b897f",marginTop:2}}>Reliance Industries Ltd &middot; NSE Bluechip</div>
              </div>
              <div style={{textAlign:"right" as const}}>
                <div style={{fontFamily:"IBM Plex Mono,monospace",fontWeight:700,fontSize:20,color:"#f2f1ec"}}>&#8377;2,941.50</div>
                <div style={{fontSize:12,color:"#35c15e",fontWeight:600}}>+1.24% today</div>
              </div>
            </div>
            <svg width={CW} height={CH} viewBox={`0 0 ${CW} ${CH}`} style={{display:"block",maxWidth:"100%",width:"100%"}}>
              <defs><linearGradient id="lp-area" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#7da0de" stopOpacity="0.22"/><stop offset="100%" stopColor="#7da0de" stopOpacity="0"/></linearGradient></defs>
              {[0.25,0.5,0.75].map(t=><line key={t} x1={8} y1={t*CH} x2={CW-8} y2={t*CH} stroke="rgba(242,241,236,0.05)" strokeWidth="1"/>)}
              {chartProgress>0.05&&<path d={partialPath+`L${8+(vCount-1)/(CHART_PRICES.length-1)*(CW-16)},${CH-8}L8,${CH-8}Z`} fill="url(#lp-area)"/>}
              <path d={partialPath} fill="none" stroke="#7da0de" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"/>
              {renderEndDot()}
            </svg>
            <div style={{display:"flex",gap:4,marginBottom:12}}>
              {["1M","3M","6M","1Y","5Y"].map((r,i)=>(
                <button key={r} style={{background:i===2?"rgba(125,160,222,0.14)":"transparent",border:"none",color:i===2?"#7da0de":"#8b897f",fontSize:11,fontWeight:600,padding:"3px 9px",borderRadius:5,cursor:"pointer",fontFamily:"IBM Plex Mono,monospace"}}>{r}</button>
              ))}
            </div>
            <div style={{display:"flex",alignItems:"center",gap:8,padding:"10px 0",borderTop:"1px solid rgba(242,241,236,0.07)"}}>
              <span style={{fontSize:11,fontWeight:700,color:"#8b897f",letterSpacing:"0.05em",textTransform:"uppercase" as const,whiteSpace:"nowrap"}}>Sentiment</span>
              <div style={{flex:1,height:4,background:"rgba(242,241,236,0.08)",borderRadius:2,overflow:"hidden"}}>
                <div style={{height:"100%",width:`${sentProg*78}%`,background:"linear-gradient(90deg,#35c15e,#7da0de)",borderRadius:2,transition:"width 1.2s cubic-bezier(.22,.68,0,1.2)"}}/>
              </div>
              <span style={{fontFamily:"IBM Plex Mono,monospace",fontSize:12,fontWeight:700,color:"#35c15e"}}>+0.78</span>
              <span style={{fontSize:10,fontWeight:700,background:"rgba(53,193,94,0.12)",color:"#35c15e",padding:"2px 7px",borderRadius:4}}>Bullish</span>
            </div>
            <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginTop:8,padding:"10px 14px",background:"rgba(53,193,94,0.07)",borderRadius:8,border:"1px solid rgba(53,193,94,0.16)"}}>
              <span style={{display:"flex",alignItems:"center",gap:6,fontSize:12,fontWeight:700,color:"#35c15e",fontFamily:"IBM Plex Mono,monospace"}}>
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none"><path d="M2 9L5 5L7 7L10 2" stroke="#35c15e" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                XGBoost &rarr; BUY
              </span>
              <span style={{fontSize:11,color:"#8b897f",fontFamily:"IBM Plex Mono,monospace"}}>Confidence: 84.3%</span>
            </div>
          </div>
        </div>
      </section>

      {/* ── FEATURES ── */}
      <section id="features" style={{position:"relative",zIndex:10,maxWidth:1200,margin:"0 auto",padding:"80px 32px"}}>
        <div style={{textAlign:"center",marginBottom:54}}>
          <div style={{fontSize:10.5,fontWeight:700,letterSpacing:"0.1em",textTransform:"uppercase" as const,color:"#8b897f",marginBottom:12}}>Platform Features</div>
          <h2 style={{fontSize:"clamp(28px,4vw,44px)",fontWeight:800,letterSpacing:"-0.03em",lineHeight:1.12,color:"#f2f1ec",margin:"0 0 14px"}}>
            Everything you need to<br/>
            <span style={{background:"linear-gradient(135deg,#7da0de,#35c15e)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",backgroundClip:"text"}}>trade with conviction</span>
          </h2>
          <p style={{fontSize:16,color:"#c3c2b7",lineHeight:1.65,maxWidth:540,margin:"0 auto"}}>A unified workspace combining ML predictions, sentiment data, and technical analysis for the NSE.</p>
        </div>
        <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fill,minmax(300px,1fr))",gap:14}}>
          {FEATURES.map((f,i)=>(
            <div key={i}
              style={{background:"rgba(23,27,33,0.75)",backdropFilter:"blur(12px)",WebkitBackdropFilter:"blur(12px)",border:hovered===i?`1px solid ${f.accent}55`:"1px solid rgba(242,241,236,0.08)",borderRadius:16,padding:"26px 22px",cursor:"default",position:"relative",overflow:"hidden",transform:hovered===i?"translateY(-6px) scale(1.015)":"translateY(0) scale(1)",boxShadow:hovered===i?`0 24px 48px -12px rgba(0,0,0,0.7),0 0 40px -8px ${f.glow}`:"0 12px 30px -18px rgba(0,0,0,0.6)",transition:"transform 0.25s cubic-bezier(.22,.68,0,1.2),box-shadow 0.25s ease,border-color 0.2s ease"}}
              onMouseEnter={()=>setHovered(i)} onMouseLeave={()=>setHovered(null)}>
              <div style={{position:"absolute",top:0,left:0,right:0,height:2,background:f.accent,borderRadius:"16px 16px 0 0",opacity:0.65}}/>
              <div style={{width:48,height:48,borderRadius:12,background:f.glow,display:"flex",alignItems:"center",justifyContent:"center",marginBottom:14}}>
                <svg width="26" height="26" viewBox="0 0 26 26" fill="none" dangerouslySetInnerHTML={{__html:ICON_SVG_CONTENT[i]}}/>
              </div>
              <div style={{fontSize:15.5,fontWeight:700,color:"#f2f1ec",marginBottom:4,letterSpacing:"-0.01em"}}>{f.label}</div>
              <div style={{fontSize:11,fontWeight:600,color:"#8b897f",letterSpacing:"0.05em",textTransform:"uppercase" as const,marginBottom:10}}>{f.sub}</div>
              <div style={{fontSize:13.5,color:"#c3c2b7",lineHeight:1.6}}>{f.desc}</div>
            </div>
          ))}
        </div>
      </section>

      {/* ── SENTIMENT SECTION ── */}
      <section style={{position:"relative",zIndex:10,background:"rgba(17,21,28,0.6)",borderTop:"1px solid rgba(242,241,236,0.08)",borderBottom:"1px solid rgba(242,241,236,0.08)",padding:"96px 32px"}}>
        <div style={{maxWidth:1200,margin:"0 auto",display:"flex",gap:80,alignItems:"center",flexWrap:"wrap" as const}}>
          <div style={{flex:"0 0 auto",maxWidth:460}}>
            <div style={{fontSize:10.5,fontWeight:700,letterSpacing:"0.1em",textTransform:"uppercase" as const,color:"#8b897f",marginBottom:12}}>Sentiment Engine</div>
            <h2 style={{fontSize:"clamp(26px,3.5vw,40px)",fontWeight:800,letterSpacing:"-0.03em",lineHeight:1.12,color:"#f2f1ec",margin:"0 0 14px"}}>
              Market mood,<br/><span style={{background:"linear-gradient(135deg,#35c15e,#7da0de)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",backgroundClip:"text"}}>quantified daily</span>
            </h2>
            <p style={{fontSize:15.5,color:"#c3c2b7",lineHeight:1.65,marginBottom:22}}>News across NSE companies is scraped, run through our NLP pipeline, and overlaid with price action so you see sentiment shifts before markets move.</p>
            {["Daily news ingestion from Indian financial media","Ticker-level sentiment scoring (-1 to +1)","Price x Sentiment correlation overlay charts","Sentiment calendar heatmap by date"].map((item,i)=>(
              <div key={i} style={{display:"flex",alignItems:"flex-start",gap:10,fontSize:14,color:"#c3c2b7",marginBottom:10,lineHeight:1.5}}>
                <span style={{color:"#35c15e",fontWeight:700,flexShrink:0,marginTop:1}}>&#10003;</span>{item}
              </div>
            ))}
          </div>
          <div style={{flex:1,minWidth:300,display:"flex",flexDirection:"column",gap:14}}>
            <div style={{background:"rgba(23,27,33,0.88)",border:"1px solid rgba(242,241,236,0.08)",borderRadius:16,padding:"20px 20px 12px"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:14}}>
                <span style={{fontSize:11.5,fontWeight:700,color:"#8b897f",letterSpacing:"0.05em",textTransform:"uppercase" as const}}>30-Day Sentiment Score</span>
                <span style={{color:"#35c15e",fontSize:12,fontWeight:700}}>&#8593; Bullish trend</span>
              </div>
              <div style={{display:"flex",gap:6,alignItems:"flex-end",height:90}}>
                {SENTIMENT_DATA.map((v,i)=>(
                  <div key={i} style={{flex:1,height:"100%",display:"flex",alignItems:"flex-end"}}>
                    <div style={{width:"100%",borderRadius:"3px 3px 0 0",minHeight:4,height:mounted?`${v*100}%`:"4%",background:v>0.6?"linear-gradient(180deg,#35c15e 0%,rgba(53,193,94,0.35) 100%)":v>0.4?"linear-gradient(180deg,#f0b429 0%,rgba(240,180,41,0.35) 100%)":"linear-gradient(180deg,#e8635f 0%,rgba(232,99,95,0.35) 100%)",transition:`height ${0.4+i*0.06}s cubic-bezier(.22,.68,0,1.2) ${i*0.05}s`}}/>
                  </div>
                ))}
              </div>
              <div style={{display:"flex",gap:6,marginTop:6}}>
                {["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"].map((m,i)=>(
                  <span key={i} style={{flex:1,fontSize:9,color:"#8b897f",textAlign:"center" as const,fontFamily:"IBM Plex Mono,monospace"}}>{m}</span>
                ))}
              </div>
            </div>
            <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:8}}>
              {([{t:"RELIANCE",s:"+0.78",c:"#35c15e"},{t:"HDFCBANK",s:"+0.42",c:"#f0b429"},{t:"INFY",s:"+0.91",c:"#35c15e"},{t:"WIPRO",s:"-0.18",c:"#e8635f"}] as const).map(chip=>(
                <div key={chip.t} style={{background:"rgba(23,27,33,0.88)",border:"1px solid rgba(242,241,236,0.08)",borderRadius:10,padding:"10px 14px",display:"flex",justifyContent:"space-between",alignItems:"center"}}>
                  <span style={{fontSize:11,fontWeight:700,color:"#8b897f",fontFamily:"IBM Plex Mono,monospace"}}>{chip.t}</span>
                  <span style={{color:chip.c,fontWeight:700,fontSize:13,fontFamily:"IBM Plex Mono,monospace"}}>{chip.s}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* ── XGBOOST SECTION ── */}
      <section style={{position:"relative",zIndex:10,padding:"96px 32px"}}>
        <div style={{maxWidth:1200,margin:"0 auto",display:"flex",gap:80,alignItems:"center",flexWrap:"wrap" as const}}>
          <div style={{flex:1,minWidth:300}}>
            <div style={{background:"rgba(23,27,33,0.88)",border:"1px solid rgba(240,180,41,0.18)",borderRadius:20,padding:"24px 22px 20px",boxShadow:"0 0 40px -10px rgba(240,180,41,0.1)"}}>
              <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",marginBottom:18}}>
                <span style={{display:"flex",alignItems:"center",gap:8,fontSize:12.5,fontWeight:700,color:"#f0b429",fontFamily:"IBM Plex Mono,monospace"}}>
                  <svg width="13" height="13" viewBox="0 0 13 13" fill="none"><path d="M1 9.5L4 5.5L6.5 7L11 1.5" stroke="#f0b429" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
                  XGBoost Prediction &mdash; Today
                </span>
                <span style={{color:"#8b897f",fontSize:11,fontFamily:"IBM Plex Mono,monospace"}}>RELIANCE.NS</span>
              </div>
              {PRED_ROWS.map((row,i)=>{
                const pct=(((row.actual-row.predicted)/row.predicted)*100).toFixed(2);
                const up=parseFloat(pct)>=0;
                return (
                  <div key={i} style={{display:"flex",alignItems:"center",gap:10,marginBottom:10}}>
                    <span style={{fontSize:10.5,color:"#8b897f",width:80,flexShrink:0,fontFamily:"IBM Plex Mono,monospace"}}>{row.label}</span>
                    <div style={{flex:1,display:"flex",flexDirection:"column",gap:3}}>
                      <div style={{height:5,background:"rgba(242,241,236,0.06)",borderRadius:2,overflow:"hidden"}}><div style={{height:"100%",width:`${(row.actual/3100)*100}%`,background:"#7da0de",borderRadius:2}}/></div>
                      <div style={{height:5,background:"rgba(242,241,236,0.06)",borderRadius:2,overflow:"hidden"}}><div style={{height:"100%",width:`${(row.predicted/3100)*100}%`,background:"#f0b429",borderRadius:2,opacity:0.85}}/></div>
                    </div>
                    <span style={{fontSize:11,fontWeight:700,fontFamily:"IBM Plex Mono,monospace",color:up?"#35c15e":"#e8635f",width:48,textAlign:"right" as const}}>{up?"+":""}{pct}%</span>
                  </div>
                );
              })}
              <div style={{display:"flex",gap:16,margin:"14px 0 10px",paddingTop:10,borderTop:"1px solid rgba(242,241,236,0.08)"}}>
                <span style={{display:"flex",alignItems:"center",gap:6,fontSize:11,color:"#8b897f"}}><span style={{width:8,height:8,borderRadius:2,background:"#7da0de",flexShrink:0,display:"inline-block"}}/>Actual</span>
                <span style={{display:"flex",alignItems:"center",gap:6,fontSize:11,color:"#8b897f"}}><span style={{width:8,height:8,borderRadius:2,background:"#f0b429",flexShrink:0,display:"inline-block"}}/>Predicted</span>
              </div>
              <div style={{display:"flex",alignItems:"center",gap:10,paddingTop:12,borderTop:"1px solid rgba(242,241,236,0.08)"}}>
                <span style={{fontSize:11,color:"#8b897f",whiteSpace:"nowrap"}}>Rolling 30-day accuracy</span>
                <div style={{flex:1,height:6,background:"rgba(242,241,236,0.08)",borderRadius:3,overflow:"hidden"}}>
                  <div style={{height:"100%",width:mounted?"76%":"0%",background:"linear-gradient(90deg,#7da0de,#35c15e)",borderRadius:3,transition:"width 1.5s cubic-bezier(.22,.68,0,1.2) 0.5s"}}/>
                </div>
                <span style={{fontSize:13,fontWeight:700,color:"#35c15e",fontFamily:"IBM Plex Mono,monospace"}}>76%</span>
              </div>
            </div>
          </div>
          <div style={{flex:"0 0 auto",maxWidth:460}}>
            <div style={{fontSize:10.5,fontWeight:700,letterSpacing:"0.1em",textTransform:"uppercase" as const,color:"#8b897f",marginBottom:12}}>ML Engine</div>
            <h2 style={{fontSize:"clamp(26px,3.5vw,40px)",fontWeight:800,letterSpacing:"-0.03em",lineHeight:1.12,color:"#f2f1ec",margin:"0 0 14px"}}>
              XGBoost-powered<br/><span style={{background:"linear-gradient(135deg,#f0b429,#e8635f)",WebkitBackgroundClip:"text",WebkitTextFillColor:"transparent",backgroundClip:"text"}}>directional signals</span>
            </h2>
            <p style={{fontSize:15.5,color:"#c3c2b7",lineHeight:1.65,marginBottom:22}}>Our XGBoost model ingests price history, volume, 20+ technical indicators, and sentiment scores to produce next-session directional forecasts with full backtesting visibility.</p>
            {["20+ feature engineering inputs","Daily retrain pipeline on Supabase Postgres","Prediction vs. actual overlay charts","Ticker-level rolling accuracy metrics"].map((item,i)=>(
              <div key={i} style={{display:"flex",alignItems:"flex-start",gap:10,fontSize:14,color:"#c3c2b7",marginBottom:10,lineHeight:1.5}}>
                <span style={{color:"#f0b429",fontWeight:700,flexShrink:0}}>&#10003;</span>{item}
              </div>
            ))}
            <Link href="/dashboard" style={{display:"inline-flex",alignItems:"center",gap:8,background:"linear-gradient(135deg,#3b5ba5 0%,#7da0de 100%)",color:"#fff",borderRadius:10,padding:"13px 26px",fontSize:14.5,fontWeight:700,textDecoration:"none",border:"1px solid rgba(255,255,255,0.2)",marginTop:26,boxShadow:"0 0 28px rgba(125,160,222,0.3)"}}>
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none"><path d="M2 10L5 6L8 7.5L13 2" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/></svg>
              Open Dashboard
            </Link>
          </div>
        </div>
      </section>

      {/* ── FOOTER ── */}
      <footer style={{position:"relative",zIndex:10,borderTop:"1px solid rgba(242,241,236,0.08)",background:"rgba(13,15,19,0.95)",padding:"28px 32px"}}>
        <div style={{maxWidth:1240,margin:"0 auto",display:"flex",alignItems:"center",justifyContent:"space-between",gap:24,flexWrap:"wrap" as const}}>
          <div style={{display:"flex",alignItems:"center",gap:10}}>
            <div style={{width:28,height:28,borderRadius:7,background:"linear-gradient(135deg,#3b5ba5,#7da0de)",display:"flex",alignItems:"center",justifyContent:"center"}}>
              <svg width="14" height="14" viewBox="0 0 16 16" fill="none"><path d="M2 12L6 7L9 9.5L14 3" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/></svg>
            </div>
            <div>
              <div style={{fontWeight:700,fontSize:14,color:"#f2f1ec"}}>Stock Analytics</div>
              <div style={{fontSize:11,color:"#8b897f"}}>by Shubham &middot; NSE Equities</div>
            </div>
          </div>
          <div style={{display:"flex",alignItems:"center",gap:6,fontSize:12,color:"#8b897f",fontFamily:"IBM Plex Mono,monospace",flexWrap:"wrap" as const}}>
            Live from Supabase Postgres &nbsp;&middot;&nbsp; stock_data &nbsp;&middot;&nbsp; final_analysis &nbsp;&middot;&nbsp; News
          </div>
          <Link href="/dashboard" style={{display:"inline-flex",alignItems:"center",gap:6,background:"linear-gradient(135deg,#3b5ba5,#7da0de)",color:"#fff",borderRadius:8,padding:"8px 18px",fontSize:13,fontWeight:600,textDecoration:"none",border:"1px solid rgba(255,255,255,0.18)"}}>
            Go to Dashboard &rarr;
          </Link>
        </div>
      </footer>
    </div>
  );
}