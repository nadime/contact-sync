#!python3

from collections import OrderedDict
import datetime
import functools
import os
import sys
import sendgrid
import time

import socket
if "asher" in socket.gethostname().lower():
    sys.path.insert(0, "../contact-sync-lib")

from contactssync.vars import *
from contactssync import (
    AirtableContact,
    CPlusContact,
    GoogleContact,
    AirtableConnection,
    CPlusConnection,
    GoogleConnection,
    Contact,
    Search,
    Comparison,
)

from secrets import *

def send_email(to, subject, body):
    sg = sendgrid.SendGridAPIClient(
        SENDGRID_API_KEY
    )

    to_email = sendgrid.To(EMAIL_ADDRESS, EMAIL_NAME)
    from_email = sendgrid.Email(EMAIL_ADDRESS, EMAIL_NAME)
    content = sendgrid.Content(
        "text/html", body
    )
    mail = sendgrid.Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())

def make_name_id(x):
    ln = getattr(x, "ln", "")
    mn = getattr(x, "mn", "")
    fn = getattr(x, "fn", "")
    return f"{ln},{fn},{mn}"

def name_sorter(x):
    return make_name_id(x)


def match_contacts(c1search, c2search):
    matches = []
    unmatch_c1 = []
    unmatch_c2 = []
    dups_c1 = []
    dups_c2 = []
    c2found = {}


    for c in sorted(c1search.contacts, key=name_sorter):
        d = {'c1': c}
        m = c2search.find(c)
        if len(m) > 0:
            d['c2'] = m[0]
            for mm in m:
                c2found[mm._id] = True
            matches.append(d)
        else:
            unmatch_c1.append(d)

    for c in sorted(c2search.contacts, key=name_sorter):
        d = {'c2': c}
        if c._id in c2found:
            continue
        unmatch_c2.append(d)

    for cs in matches:
        if 'c2' in cs and isinstance(cs['c2'], list) and len(cs['c2']) > 1:
            dups_c2.append(cs['c2'])
        if 'c1' in cs and isinstance(cs['c1'], list) and len(cs['c1']) > 1:
            dups_c1.append(cs['c1'])

    return matches, unmatch_c2, unmatch_c1, dups_c2, dups_c1

def find_dups(search):
    dups = OrderedDict()
    for c in sorted(search.contacts, key=name_sorter):
        nid = make_name_id(c)
        if nid not in dups:
            dups[nid] = []
        dups[nid].append(c)
    delkeys = []
    for k in dups.keys():
        if len(dups[k]) <= 1:
            delkeys.append(k)
    for k in delkeys:
        del dups[k]
    return dups

def delete(ctx, del_contacts):
    errors = []
    deleted = []
    for cs in del_contacts.values():
        to_delete = sorted(cs, key=lambda x: x.createddt)
        num = 0
        for c in to_delete[1:]:
            try:
                result = ctx.delete(c)
                print(f"Deleted {c.fn} {c.ln}")
                if result is not None:
                    deleted.append(c)
            except Exception as e:
                errors.append(str(e))
                raise
    return deleted, errors

def add(ctx, new_contacts):
    added = []
    errors = []
    for c in new_contacts:
        try:
            result = ctx.create(ctx.contact_to_dict(c))
            print(f"Added {c.fn} {c.ln}")
            if result is not None:
                added.append(c)
        except Exception as e:
            errors.append(str(e))
            raise
    return added, errors


def edit(ctx, edit_contacts, left_right, update_delay=0):
    edited = []
    skipped = []
    errors = []
    for c1, c2 in edit_contacts:
        compared, results, _ = c1.compare(c2)
        if compared:
            skipped.append(c1)
            continue
        c3, _, _ = Contact.resolve(c1, c2, take_ids=left_right)
        c3.dedup()
        if c3.fn is not None:
            c3.fn = c3.fn.strip()
        if c3.ln is not None:
            c3.ln = c3.ln.strip()
        if left_right == Comparison.Left:
            compare_contact = c1
            other_contact = c2
        elif left_right == Comparison.Right:
            compare_contact = c2
            other_contact = c1
        else:
            raise Exception("invalid argument for 'left_right' must be left or right")
        compared, results, compare_values = compare_contact.compare(c3)
        # resolved contacts aren't necessarily different
        if compared or all([ x != Comparison.Right for x in results.values()]):
            skipped.append(c3)
            continue
        try:
            ctx.update(c3)
            result_response = {}
            for attrname in results.keys():
                result_response[attrname] = compare_values[attrname]
            print(f"{len(edited)+1}: {c3.fn} {c3.ln} {compared} {result_response}")
            c3._fs = other_contact._fs
            edited.append((c3,compare_contact))
            if update_delay > 0:
                time.sleep(update_delay)
        except Exception as e:
            errors.append(e)
            if len(errors) > 5000:
                print(f"Total listed for possible inclusion: {len(edit_contacts)}")
                print(f"Total skipped: {len(skipped)}")
                print(f"Total edited so far: {len(edited)}")
                raise Exception(
                    "Found several errors while editing contacts:\n","\n".join(
                        [str(e) for e in errors]
                    )
                )
    return edited, errors

def get_ctx(ctxname):
    return {
        "google": functools.partial(GoogleConnection,token_file_or_path=GOOGLE_TOKEN),
        "airtable": functools.partial(AirtableConnection, BASE_NAME, TABLE_NAME, AIRTABLE_API_KEY),
    }[ctxname.lower()]

def create_changes_body(added, edited, deleted):
    body = ""
    for c,where in deleted:
        body += f"[Deleted from {where}]<br>"
        body += c.to_series(ignore_null=True).to_frame().style.to_html() + "<p>"

    for c,where in added:
        body += f"[Added to {where}]<br>"
        body += c.to_series(ignore_null=True).to_frame().style.to_html() + "<p>"

    for tup,where in edited:
        edited_c = tup[0]
        c2 = tup[1]
        visual_styler = Contact.compare_visual(edited_c,c2)
        if visual_styler is None:
            continue
        body += f"[Edited in {where}]<br>"
        body += visual_styler.to_html() + "<p>"

    return body

def main(fn,ln):
    c1str = "Airtable"
    c2str = "Google"
    c1ctx = get_ctx(c1str)()
    c2ctx = get_ctx(c2str)()
    if ln is None:
        print("No provided name, operating on ALL contacts")
        c1search = Search(c1ctx.list())
        c2search = Search(c2ctx.list())
    else:
        print(f"Working on {fn} {ln}")
        c1search = Search(c1ctx.get_by_name(fn,ln))
        c2search = Search(c2ctx.get_by_name(fn,ln))

    matches, just_c1, just_c2, dup_c1, dup_c2 = match_contacts(
        c1search, c2search)

    dups_c1 = find_dups(c1search)
    print(f"DUPS: C1 {len(dups_c1)}")
    dups_c2 = find_dups(c2search)
    print(f"DUPS: C2 {len(dups_c2)}")
    print(f"Deleting from {c1str}")
    deleted_c1, errors = delete(c1ctx, dups_c1)
    if len(errors):
        print(f"DELETE C1-{c1str.upper()} ERRORS {len(errors)}")
        print(errors)
    print(f"Deleting from {c2str}")
    deleted_c2, errors = delete(c2ctx, dups_c2)
    if len(errors):
        print(f"DELETE C2-{c2str.upper()} ERRORS {len(errors)}")
        print(errors)

    print(f"Adding to {c1str}")
    added_c1, errors = add(c1ctx,[d["c2"] for d in just_c1])
    if len(errors):
        print(f"ADD C1-{c1str.upper()} ERRORS ({len(errors)}):")
        print(errors)
    print(f"Editing {c1str}")
    edited_c1, errors = edit(
        c1ctx,[(d["c1"], d["c2"]) for d in matches], Comparison.Left
    )
    if len(errors):
        print(f"EDIT C1-{c1str.upper()} ERRORS ({len(errors)}):")
        print(errors)
    print(f"Adding to {c2str}")
    added_c2, errors = add(c2ctx, [d["c1"] for d in just_c2])
    if len(errors):
        print(f"ADD C2-{c2str.upper()} ERRORS ({len(errors)}):")
        print(errors)
    print(f"Editing {c2str}")
    edited_c2, errors = edit(
        c2ctx,
        [(d["c1"], d["c2"]) for d in matches],
        Comparison.Right,
        update_delay=1
    )
    if len(errors):
        print(f"EDIT C2-{c2str.upper()} ERRORS ({len(errors)}):")
        print(errors)

    stats = ""
    stats += "=== STATS ===\n"
    stats += f"{len(matches)} matches\n"
    stats += f"{len(just_c1)} ONLY in C1 ({c1str})\n"
    stats += f"{len(just_c2)} ONLY IN C2 ({c2str})\n"
    stats += f"{len(dup_c1)} C1 DUPS ({c1str})\n"
    stats += f"{len(dup_c2)} C2 DUPS ({c2str})\n"
    stats += "==============\n\n"
    stats += f"{len(added_c1)} added to C1 ({c1str})\n"
    stats += f"{len(edited_c1)} edited in C1 ({c1str})\n"
    stats += f"{len(added_c2)} added to C2 ({c2str})\n"
    stats += f"{len(edited_c2)} edited in c2 ({c2str})\n"

    dt = datetime.datetime.now()

    changes = len(added_c1) + len(added_c2) + len(edited_c1) + len(edited_c2) + len(deleted_c1) + len(deleted_c2)
    subject = f"{c1str} <=> {c2str} sync changes={changes} ({dt})"
    if changes > 50:
        stats = f"Too many changes to break down with graphics ({changes})<p>"
        stats += "\n"
        added = [ c for c in added_c1 ]
        added.extend([c for c in added_c2 ])
        stats += f"<p>added to {c1str}:<br>"
        for ind,add_c in enumerate(added_c1):
            stats += f"{ind+1}: {add_c.fn} {add_c.ln}<br>"
        stats += f"<p>added to {c2str}:<br>"
        for ind,add_c in enumerate(added_c2):
            stats += f"{ind+1}: {add_c.fn} {add_c.ln}<br>"
        stats += f"<p>edited in {c1str}:<br>"
        for edit_c, old_c in edited_c1:
            compared, results, compare_values = edit_c.compare(old_c)
            result_response = {}
            for attrname in results.keys():
                result_response[attrname] = compare_values[attrname]
            stats += f"{old_c.fn} {old_c.ln} {compared} {result_response}<p>"
        stats += f"<p>edited in {c2str}:<br>"
        for edit_c, old_c in edited_c2:
            compared, results, compare_values = edit_c.compare(old_c)
            result_response = {}
            for attrname in results.keys():
                result_response[attrname] = compare_values[attrname]
            stats += f"{old_c.fn} {old_c.ln} {compared} {result_response}<p>"
        body = stats.replace("\n","<br>") + "<br>=====<p>"
        body = f"contactssync version: {contactssync.__version__}"
        send_email(
            EMAIL_ADDRESS,
            subject,
            body,
        )
    elif changes > 0:
        added = [(c, f"{c1str}") for c in added_c1 ]
        added.extend([(c, f"{c2str}") for c in added_c2 ])
        edited = [(tup, f"{c1str}") for tup in edited_c1 ]
        edited.extend([(tup, f"{c2str}") for tup in edited_c2 ])
        deleted = [(c, f"{c1str}") for c in deleted_c1 ]
        deleted.extend([(c, f"{c2str}") for c in deleted_c2])
        body = create_changes_body(added, edited, deleted)
        body = stats.replace("\n","<br>") + "<br>=====<p>" + body
        body = f"contactssync version: {contactssync.__version__}"
        send_email(
            EMAIL_ADDRESS,
            subject,
            body
        )
    #print(stats)

if __name__ == "__main__":
    fn = None; ln = None
    argc = len(sys.argv)
    if argc > 1:
        fn = sys.argv[1]
        ln = sys.argv[2]
    main(fn=fn, ln=ln)
