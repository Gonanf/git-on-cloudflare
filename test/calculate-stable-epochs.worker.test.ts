import { it, expect } from "vitest";
import { calculateStableEpochs } from "@/do/repo/packs.ts";

it("stable-epochs: preserves entire boundary-crossing epoch (soft cap)", () => {
  const last = "do/x/objects/pack/pack-999.pack";
  const e2 = [
    "do/x/objects/pack/pack-hydr-e200-1.pack",
    "do/x/objects/pack/pack-hydr-e200-2.pack",
    "do/x/objects/pack/pack-hydr-e200-3.pack",
  ];
  const e1 = [
    "do/x/objects/pack/pack-hydr-e100-1.pack",
    "do/x/objects/pack/pack-hydr-e100-2.pack",
    "do/x/objects/pack/pack-hydr-e100-3.pack",
  ];
  const normals = [
    "do/x/objects/pack/pack-998.pack",
    "do/x/objects/pack/pack-997.pack",
    "do/x/objects/pack/pack-996.pack",
  ];
  const packList = [last, ...e2, normals[0], ...e1, normals[1], normals[2]];

  // keepPacks smaller than last(1)+epoch(3) -> soft include entire epoch => 4 kept
  const { stableEpochs, keepSet } = calculateStableEpochs(packList, 3, last);
  expect(stableEpochs).toEqual(["e200"]);
  const kept = packList.filter((k) => keepSet.has(k));
  expect(kept).toEqual([last, ...e2]);
});

it("stable-epochs: ignores legacy hydras and keeps normals when space is tight", () => {
  const last = "do/x/objects/pack/pack-50.pack";
  const legacy = ["do/x/objects/pack/pack-hydr-1.pack", "do/x/objects/pack/pack-hydr-2.pack"];
  const normals = [
    "do/x/objects/pack/pack-49.pack",
    "do/x/objects/pack/pack-48.pack",
    "do/x/objects/pack/pack-47.pack",
  ];
  const packList = [last, legacy[0], normals[0], legacy[1], normals[1], normals[2]];
  const { stableEpochs, keepSet } = calculateStableEpochs(packList, 3, last);
  expect(stableEpochs).toEqual([]);
  const kept = packList.filter((k) => keepSet.has(k));
  // Should keep last + the next 2 normals, legacy hydras are de-prioritized
  expect(kept).toEqual([last, normals[0], normals[1]]);
});

it("stable-epochs: only newest epoch is stable when keep horizon is small", () => {
  const last = "do/x/objects/pack/pack-77.pack";
  const e3 = ["do/x/objects/pack/pack-hydr-e300-1.pack", "do/x/objects/pack/pack-hydr-e300-2.pack"];
  const e2 = ["do/x/objects/pack/pack-hydr-e200-1.pack", "do/x/objects/pack/pack-hydr-e200-2.pack"];
  const e1 = ["do/x/objects/pack/pack-hydr-e100-1.pack", "do/x/objects/pack/pack-hydr-e100-2.pack"];
  const packList = [last, ...e3, ...e2, ...e1];
  const { stableEpochs, keepSet } = calculateStableEpochs(packList, 3, last);
  expect(stableEpochs).toEqual(["e300"]);
  const kept = packList.filter((k) => keepSet.has(k));
  expect(kept).toEqual([last, ...e3]);
});
