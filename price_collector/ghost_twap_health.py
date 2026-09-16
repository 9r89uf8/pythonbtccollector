"""Bounded receipt-clock feed diagnostics for the optional ghost worker."""
from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy

HOUR_MS = 3_600_000
MAX_HOURS = 49


class GhostFeedHealth:
    def __init__(self, run_id: str, started_ms: int):
        self.run_id, self.started_ms = run_id, started_ms
        self.hours: OrderedDict = OrderedDict()
        self.last = {}
        self.source_max = {}
        self.dropped_hours = 0
        self.history_evictions = 0

    def _hour(self, now_ms: int) -> dict:
        hour = now_ms // HOUR_MS * HOUR_MS
        # Fill silent hours too: absence of accepted events is evidence.
        previous = next(reversed(self.hours)) if self.hours else hour-HOUR_MS
        first = max(previous+HOUR_MS, hour-(MAX_HOURS-1)*HOUR_MS)
        if first > previous+HOUR_MS:
            self.dropped_hours += (first-previous-HOUR_MS)//HOUR_MS
        for stamp in range(first, hour+HOUR_MS, HOUR_MS):
            self.hours[stamp] = dict(hour_start_ms=stamp, revision=0,
                observed_from_ms=max(stamp,self.started_ms), observed_until_ms=stamp,
                complete=False, feeds={})
        while len(self.hours) > MAX_HOURS:
            self.hours.popitem(last=False)
            self.history_evictions += 1
        # Clock regressions are handled by the owning runtime. Do not rewrite
        # an evicted historical bucket if a rejected event arrives late.
        return self.hours.get(hour) or self.hours[next(reversed(self.hours))]

    @staticmethod
    def _feed(bucket: dict, feed: str) -> dict:
        return bucket['feeds'].setdefault(feed, dict(events=0, gap_events=0,
            connection_ends=0, max_receipt_gap_ns=0, gaps_over_3s=0,
            gaps_over_5s=0, gap_over_3s_total_ns=0, gap_over_5s_total_ns=0,
            source_stamp_holes=0, late_source_events=0, duplicate_source_events=0,
            future_source_events=0, max_source_age_ms=0))

    def observe(self, event) -> None:
        bucket = self._hour(event.received_wall_ns//1_000_000)
        values = self._feed(bucket,event.feed)
        values['events'] += 1
        previous = self.last.get(event.feed)
        if previous is not None:
            elapsed = event.received_monotonic_ns-previous['monotonic_ns']
            if elapsed >= 0:
                values['max_receipt_gap_ns'] = max(values['max_receipt_gap_ns'],elapsed)
                for seconds in (3,5):
                    if elapsed > seconds*1_000_000_000:
                        values[f'gaps_over_{seconds}s'] += 1
                        values[f'gap_over_{seconds}s_total_ns'] += elapsed
        source = event.source_timestamp_ms
        age = event.received_wall_ns//1_000_000-source
        if age < 0:
            values['future_source_events'] += 1
        else:
            values['max_source_age_ms'] = max(values['max_source_age_ms'],age)
            old_source = self.source_max.get(event.feed)
            if old_source is not None:
                if source < old_source:
                    values['late_source_events'] += 1
                elif source == old_source:
                    values['duplicate_source_events'] += 1
                else:
                    values['source_stamp_holes'] += max(0,(source-old_source)//1000-1)
            self.source_max[event.feed] = max(source,old_source or source)
        self.last[event.feed] = dict(monotonic_ns=event.received_monotonic_ns,
            wall_ns=event.received_wall_ns, source_ms=source)
        bucket['revision'] += 1

    def note_gap(self, feed: str, reason: str, wall_ms: int) -> None:
        bucket = self._hour(wall_ms)
        values = self._feed(bucket,feed)
        values['gap_events'] += 1
        values['connection_ends'] += int(reason == 'connection_end')
        bucket['revision'] += 1

    def snapshot(self, now_ms: int, mono_ns: int) -> dict:
        self._hour(now_ms)
        result = []
        for hour,bucket in self.hours.items():
            until = min(max(now_ms,hour),hour+HOUR_MS)
            complete = self.started_ms <= hour and now_ms >= hour+HOUR_MS
            if bucket['observed_until_ms'] != until or bucket['complete'] != complete:
                bucket.update(observed_until_ms=until,complete=complete,
                              revision=bucket['revision']+1)
            result.append(deepcopy(bucket))
        ages = {feed:dict(receipt_age_ns=max(0,mono_ns-value['monotonic_ns']),
                         source_age_ms=now_ms-value['source_ms'])
                for feed,value in self.last.items()}
        return dict(run_id=self.run_id,hours=result,dropped_hours=self.dropped_hours,
            history_evictions=self.history_evictions,
            current_ages=ages,
            gap_assignment='Whole monotonic inter-arrival intervals are assigned to the ending receipt hour; threshold totals overlap.',
            source_holes_meaning='Locally unobserved source seconds, not proof of publisher nonpublication.')
