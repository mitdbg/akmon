// A state as an icon and a word, drawn the same way on every view (rule 2:
// colour never carries status alone).
import {
  CheckCircleFilled, CloseCircleFilled, ExclamationCircleFilled, MinusCircleOutlined,
  StopOutlined, SyncOutlined,
} from "@operator-ui/console/components/icons";
import type { Glyph, IconName } from "@operator-ui/console/model/shared/status";

/** The six state icons. Named in the model, drawn here, so the pure module
 *  stays free of React and every state is drawn the same way everywhere. */
const ICONS: Record<IconName, typeof CheckCircleFilled> = {
  check: CheckCircleFilled,
  close: CloseCircleFilled,
  sync: SyncOutlined,
  minus: MinusCircleOutlined,
  stop: StopOutlined,
  warning: ExclamationCircleFilled,
};

export function StateIcon({ icon }: { icon: IconName }) {
  const Icon = ICONS[icon];
  return <Icon className="okg-console__icon" aria-hidden="true" />;
}


export function Status({ mark }: { mark: Glyph }) {
  return (
    <span className="okg-console__status" data-severity={mark.severity}>
      <StateIcon icon={mark.icon} />
      <span className="okg-console__status-word">{mark.word}</span>
    </span>
  );
}
