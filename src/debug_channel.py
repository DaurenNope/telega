import json
from datetime import datetime, timedelta, timezone

# Test inserting to Google Sheets if requested
if test_sheets and gs_client and all_messages:
    console.print("\n[bold]Testing Google Sheets insertion...[/]")
    
    # First test connection
    if not gs_client.test_connection():
        console.print("[red]Failed to connect to Google Sheets.[/]")
        return
        
    # Test with regular messages first
    try:
        console.print("[dim]Testing with normal messages...[/]")
        normal_messages = [m for m in all_messages 
                         if m["text"] != "[Media message]" 
                         and len(m["text"]) < 1000
                         and not any(ord(c) > 1000 for c in m["text"])]
        
        if normal_messages:
            # First try with a single message
            if len(normal_messages) > 0:
                console.print("[dim]Testing with a single normal message first...[/]")
                single_test = normal_messages[0]
                try:
                    # Format timestamp 
                    timestamp = single_test["timestamp"].astimezone(timezone(timedelta(hours=5)))
                    formatted_timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + timestamp.strftime("%z")
                    formatted_timestamp = formatted_timestamp[:-2] + ":" + formatted_timestamp[-2:]
                    
                    test_row = [
                        formatted_timestamp,
                        single_test["channel"][:50],
                        single_test["text"].replace("\n", " ")[:495],
                        single_test["link"]
                    ]
                    
                    # Try append_row which should be more reliable
                    gs_client._execute_with_retry(
                        lambda: gs_client.sheet.append_row(test_row, table_range="A1:D1")
                    )
                    console.print("[green]✓ Single message test passed[/]")
                except Exception as single_err:
                    console.print(f"[red]Single message test failed: {str(single_err)}[/]")
                    console.print("[yellow]This indicates there may be issues with the Google Sheets API connection or format[/]")
                    # Save the problematic message to a file for inspection
                    with open(f"{channel_name}_problem_msg.json", "w", encoding="utf-8") as f:
                        json.dump(single_test, f, indent=2, default=str)
                    console.print(f"[yellow]Saved problematic message to {channel_name}_problem_msg.json for inspection[/]")
                    return
            
            # Now try batch with a few messages
            test_batch = normal_messages[:min(5, len(normal_messages))]  # Just use up to 5
            console.print(f"[dim]Testing batch append with {len(test_batch)} normal messages...[/]")
            added = gs_client.batch_append(test_batch)
            console.print(f"[green]Successfully added {added} normal messages in batch[/]")
        
        # Now test with potentially problematic messages
        if problematic_messages:
            console.print("[dim]Testing with problematic messages (one at a time)...[/]")
            
            for i, item in enumerate(problematic_messages[:3]):  # Test first 3
                try:
                    msg = item["message"]
                    console.print(f"[dim]Testing problematic message {i+1}...[/]")
                    console.print(f"[dim]Issues: {', '.join(item['reasons'])}")
                    
                    # Test one by one for better error isolation
                    added = gs_client.batch_append([msg])
                    if added > 0:
                        console.print(f"[green]Successfully added problematic message {i+1}[/]")
                    else:
                        console.print(f"[yellow]Failed to add problematic message {i+1}[/]")
                        
                        # Try direct insertion with maximum sanitization
                        try:
                            console.print("[dim]Attempting direct insertion with maximum sanitization...[/]")
                            
                            # Extra sanitization for problematic message
                            timestamp = msg["timestamp"].astimezone(timezone(timedelta(hours=5)))
                            formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # Simplified timestamp format
                            
                            # Strong sanitization for text - remove all non-ASCII
                            text = ''.join(c for c in msg["text"] if ord(c) < 128)
                            text = text.replace("\n", " ")[:100]  # Severely truncate
                            
                            safe_row = [
                                formatted_timestamp,
                                msg["channel"][:20],
                                f"[Sanitized] {text}...",
                                msg["link"]
                            ]
                            
                            gs_client._execute_with_retry(
                                lambda: gs_client.sheet.append_row(safe_row, table_range="A1:D1")
                            )
                            console.print("[green]✓ Succeeded with heavily sanitized content[/]")
                        except Exception as direct_err:
                            console.print(f"[red]Direct insertion also failed: {str(direct_err)}[/]")
                except Exception as e:
                    console.print(f"[red]Error adding problematic message {i+1}: {str(e)}[/]")
                    
                    # Save details for debugging
                    with open(f"{channel_name}_problem_{i+1}.json", "w", encoding="utf-8") as f:
                        json.dump(item, f, indent=2, default=str)
                    console.print(f"[yellow]Saved problematic message details to {channel_name}_problem_{i+1}.json[/]")
    
    except Exception as e:
        console.print(f"[red]Error during Google Sheets testing: {str(e)}[/]") 