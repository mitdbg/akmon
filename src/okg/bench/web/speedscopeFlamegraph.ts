export interface SpeedscopeFile {
  $schema?: string;
  shared?: {
    frames?: Array<{
      name?: string;
      file?: string;
      line?: number;
      col?: number;
    }>;
  };
  profiles?: SpeedscopeProfile[];
}

type SpeedscopeProfile = SpeedscopeEventedProfile | SpeedscopeSampledProfile;

interface SpeedscopeBaseProfile {
  name?: string;
  type?: string;
  unit?: string;
  startValue?: number;
  endValue?: number;
}

interface SpeedscopeEventedProfile extends SpeedscopeBaseProfile {
  type: "evented";
  events: Array<{
    type: "O" | "C";
    frame: number;
    at: number;
  }>;
}

interface SpeedscopeSampledProfile extends SpeedscopeBaseProfile {
  type: "sampled";
  samples: number[][];
  weights?: number[];
}

export interface FlamegraphNode {
  name: string;
  value: number;
  raw_value?: number;
  trace_id?: string;
  span_id?: string;
  gap_id?: string;
  tooltip?: string;
  children?: FlamegraphNode[];
}

export interface ConvertedFlamegraph {
  root: FlamegraphNode;
  profileName: string;
  profileType: string;
  unit: string;
  frameCount: number;
  totalValue: number;
  truncated?: boolean;
  omittedTraceCount?: number;
  omittedSpanCount?: number;
  attributedDuration?: number;
  unattributedDuration?: number;
  attributionPct?: number;
  normalizationMode?: "none" | "scale_overlapping_children";
  overlapNodeCount?: number;
  overlapDuration?: number;
}

interface MutableFlamegraphNode extends FlamegraphNode {
  children?: MutableFlamegraphNode[];
}

interface OpenFrame {
  node: MutableFlamegraphNode;
  frameIndex: number;
  openedAt: number;
}

function profilePriority(profile: SpeedscopeProfile): number {
  if (profile.type === "sampled") return 0;
  if (profile.type === "evented") return 1;
  return 2;
}

function frameName(file: SpeedscopeFile, frameIndex: number): string {
  const frame = file.shared?.frames?.[frameIndex];
  return frame?.name || `frame ${frameIndex}`;
}

function frameTooltip(file: SpeedscopeFile, frameIndex: number): string {
  const frame = file.shared?.frames?.[frameIndex];
  if (!frame) return `frame ${frameIndex}`;
  const location = [
    frame.file,
    typeof frame.line === "number" ? frame.line : null,
    typeof frame.col === "number" ? frame.col : null,
  ].filter((part) => part !== null && part !== undefined && part !== "");
  return location.length > 0 ? `${frame.name ?? `frame ${frameIndex}`} ${location.join(":")}` : frame.name ?? `frame ${frameIndex}`;
}

function childFor(
  parent: MutableFlamegraphNode,
  name: string,
  tooltip: string,
): MutableFlamegraphNode {
  parent.children ??= [];
  let child = parent.children.find((candidate) => candidate.name === name);
  if (!child) {
    child = { name, value: 0, tooltip };
    parent.children.push(child);
  }
  return child;
}

function convertSampledProfile(
  file: SpeedscopeFile,
  profile: SpeedscopeSampledProfile,
): MutableFlamegraphNode {
  const root: MutableFlamegraphNode = {
    name: profile.name || "speedscope sampled profile",
    value: 0,
  };
  profile.samples.forEach((sample, index) => {
    const weight = profile.weights?.[index] ?? 1;
    if (!Number.isFinite(weight) || weight <= 0) return;
    root.value += weight;
    let parent = root;
    for (const frameIndex of sample) {
      const node = childFor(parent, frameName(file, frameIndex), frameTooltip(file, frameIndex));
      node.value += weight;
      parent = node;
    }
  });
  return root;
}

function convertEventedProfile(
  file: SpeedscopeFile,
  profile: SpeedscopeEventedProfile,
): MutableFlamegraphNode {
  const root: MutableFlamegraphNode = {
    name: profile.name || "speedscope evented profile",
    value: Math.max(0, (profile.endValue ?? 0) - (profile.startValue ?? 0)),
  };
  const stack: OpenFrame[] = [];
  for (const event of profile.events) {
    if (event.type === "O") {
      const parent = stack.at(-1)?.node ?? root;
      const node = childFor(parent, frameName(file, event.frame), frameTooltip(file, event.frame));
      stack.push({ frameIndex: event.frame, node, openedAt: event.at });
      continue;
    }

    let openIndex = -1;
    for (let index = stack.length - 1; index >= 0; index -= 1) {
      if (stack[index]?.frameIndex === event.frame) {
        openIndex = index;
        break;
      }
    }
    if (openIndex === -1) continue;
    const [open] = stack.splice(openIndex);
    const duration = Math.max(0, event.at - open.openedAt);
    if (Number.isFinite(duration) && duration > 0) {
      open.node.value += duration;
    }
  }
  const childTotal = root.children?.reduce((sum, child) => sum + child.value, 0) ?? 0;
  root.value = Math.max(root.value, childTotal);
  return root;
}

export function convertSpeedscopeToFlamegraph(file: SpeedscopeFile): ConvertedFlamegraph {
  const profile = [...(file.profiles ?? [])].sort(
    (left, right) => profilePriority(left) - profilePriority(right),
  )[0];
  if (!profile) {
    throw new Error("Speedscope artifact has no profiles.");
  }

  const root = profile.type === "sampled"
    ? convertSampledProfile(file, profile as SpeedscopeSampledProfile)
    : profile.type === "evented"
      ? convertEventedProfile(file, profile as SpeedscopeEventedProfile)
      : null;

  if (!root) {
    throw new Error(`Unsupported Speedscope profile type: ${profile.type ?? "unknown"}.`);
  }
  if (!root.children?.length || root.value <= 0) {
    throw new Error("Speedscope profile has no positive-weight frames.");
  }

  return {
    root,
    profileName: profile.name || "speedscope profile",
    profileType: `${profile.type ?? "unknown"} profile`,
    unit: profile.unit || "samples",
    frameCount: file.shared?.frames?.length ?? 0,
    totalValue: root.value,
  };
}
