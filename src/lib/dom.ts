/**
 * Small DOM helpers shared by views (backlog phase 4.1).
 *
 * Kept separate from the design system's own helpers: these encode *this app's*
 * conventions, not the package's.
 */

/**
 * Show a button as working, and stop it being pressed again.
 *
 * `.is-loading` is the design system's loading affordance; disabling alongside
 * it is what actually prevents a double submit, since the class is cosmetic.
 */
export function setButtonBusy(button: HTMLButtonElement, busy: boolean): void {
  button.classList.toggle("is-loading", busy);
  button.disabled = busy;
}
