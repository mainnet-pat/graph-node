import { Trigger } from "../generated/Contract/Contract";
import { Call } from "../generated/schema"

export function handleTrigger(event: Trigger): void {
  let call = new Call("something")
  call.value = "another thing"
  call.save()
  assert(false)
}
